////////////////////////////////////////////////////////////////////////////////

// Package explode wraps all needed logic to run a parallel job that will take
// a directory of tf records and explode them into their respective classes.
// Each record found is split into its constituent record pieces and are streamed
// into the output directory based on the classes requested in the `--filter`.
package explode

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"time"

	"google.golang.org/api/iterator"

	"github.com/recogni/sorten/gcloud"
	"github.com/recogni/sorten/jobs"
	"github.com/recogni/sorten/logger"

	"github.com/recogni/tfutils"
	tf "tensorflow/core/example"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		// Command line args common to all commands
		inputDir  string
		outputDir string
		filter    string

		// Private stuff
		numWorkers int
		classes    []int
	}{}
)

////////////////////////////////////////////////////////////////////////////////

type AtomicClassNamer struct {
	*sync.RWMutex
	m map[int]int
}

// GetNextIndexForClass returns the next valid integer value for the next tf
// record to be named in a given directory of said `id`.
func (a *AtomicClassNamer) GetNextIndexForClass(id int) int {
	a.Lock()
	defer a.Unlock()

	if count, ok := a.m[id]; ok {
		a.m[id] += 1
		return count
	}

	a.m[id] = 1
	return 0
}

var acn = &AtomicClassNamer{
	RWMutex: &sync.RWMutex{},
	m:       map[int]int{},
}

////////////////////////////////////////////////////////////////////////////////

func emitTfRecord(workerId int, file string, class int, rec *tf.Features, wl *logger.WorkerLogger) error {
	wl.Log(workerId, "Got emit record for class %d (%s)\n", class, file)
	if !jobs.RangeContainsClass(CLI.classes, class) {
		return nil
	}

	outFileName := fmt.Sprintf("%05d.tfrecord", acn.GetNextIndexForClass(class))
	outItems := []string{CLI.outputDir, fmt.Sprintf("class_%05d", class), outFileName}
	outDir := strings.Join(outItems[:2], string(filepath.Separator))
	outFile := strings.Join(outItems, string(filepath.Separator))

	destination := outFile
	if gcloud.IsBucketPath(CLI.outputDir) {
		destination = path.Join(os.TempDir(), fmt.Sprintf("worker_%d.tfrecord", workerId))
	} else {
		if err := jobs.SafeMkdir(outDir); err != nil {
			return err
		}
	}

	w, err := tfutils.NewWriter(destination, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	bs, err := tfutils.GetTFRecordStringForFeatures(rec)
	if err != nil {
		return err
	}

	err = w.WriteRecord(bs)
	if err != nil {
		return err
	}

	// Need to upload the file to the bucket destination.
	if destination != outFile {
		bp, err := gcloud.NewBucketPath(outFile)
		if err != nil {
			return err
		}

		bm, err := gcloud.GetBucketManager(bp.Bucket)
		if err != nil {
			return err
		}
		return bm.BucketUploadFile(bp, destination)
	}

	return nil
}

func explodeTfWorker(workerId int, q <-chan string, wg *sync.WaitGroup, wl *logger.WorkerLogger) {
	wg.Add(1)

	// Tag the wait group since we are a new worker ready to go!
	wl.Log(workerId, "is ready for work!")
	for file := range q {
		wl.Log(workerId, "is working on file: %s", file)
		time.Sleep(1 * time.Second)
		source := file
		if gcloud.IsBucketPath(file) {
			source = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_input.tfrecord", workerId))
			bp, _ := gcloud.NewBucketPath(file)
			bm, _ := gcloud.GetBucketManager(bp.Bucket)

			wl.Log(workerId, "attempting to download %s -> %s", file, source)
			if err := bm.BucketDownloadFile(source, bp); err != nil {
				wl.Log(workerId, "Warning: unable to download %s from bucket, error: %s", file, err.Error())
			} else {
				wl.Log(workerId, "download successful!")
			}
		}

		// `source` points to the file (either downloaded from gs:// or actual).
		r, err := tfutils.NewReader([]string{source}, nil)
		if err != nil {
			wl.Log(workerId, "Error: unable to read tfrecord file. Error: %s", err.Error())
			continue
		}

		for {
			rbs, err := r.ReadRecord()
			if err == io.EOF {
				break
			} else if err != nil {
				wl.Log(workerId, "  Error: unable to read from the reader. Error: %s", err.Error())
				continue
			}

			rec, err := tfutils.GetFeatureMapFromTFRecord(rbs)
			if err != nil {
				wl.Log(workerId, "Error: unable to convert into feature. Error: %s", err.Error())
				continue
			}

			for k, v := range rec.Feature {
				if k == "image/class/label" {
					if il, ok := v.Kind.(*tf.Feature_Int64List); ok {
						vs := il.Int64List.Value
						if len(vs) > 0 {
							if err := emitTfRecord(workerId, file, int(vs[0]), rec, wl); err != nil {
								wl.Log(workerId, "Error: unable to emit record (%s). Error: %s", file, err.Error())
							}
						}
					}
					break
				}
			}
		}
	}

	// File queue is exhausted, we are done!
	wl.Log(workerId, "Worker is now done")
	wg.Done()
}

func queueFileForWorker(q chan<- string, fp string) {
	// if strings.ToLower(path.Ext(fp)) == ".tfrecord" {
	q <- fp
	// }
}

////////////////////////////////////////////////////////////////////////////////

func RunJob(nworkers int, args []string, wl *logger.WorkerLogger) error {
	// Setup args to parse.
	fs := flag.NewFlagSet("ExplodeTf", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to write images to")
	fs.StringVar(&CLI.filter, "filter", "", "specify classes to include, empty == include all")
	fs.Parse(args)

	// Validate input arguments.
	if len(CLI.inputDir) == 0 {
		return errors.New("specify input directory with --input")
	}
	if len(CLI.outputDir) == 0 {
		return errors.New("specify output directory with --output")
	}
	if !gcloud.IsBucketPath(CLI.outputDir) {
		if err := jobs.SafeMkdir(CLI.outputDir); err != nil {
			return err
		}
	}

	// Setup internal / constructed arguments.
	var err error
	CLI.classes, err = jobs.ParseRangeString(CLI.filter)
	if err != nil {
		return err
	}
	CLI.numWorkers = nworkers

	// Create synchronization primitives.
	var wg sync.WaitGroup
	fileq := make(chan string, CLI.numWorkers)

	// Create workers, each one is responsible for decrementing his waitgroup
	// after it has exhausted the work queue.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		go explodeTfWorker(wId, fileq, &wg, wl)
	}

	// TODO: SHABA_BOILERPLATE_ITERATOR
	// The below code is SUPER boilerplate! and can be abstracted out fi the
	// signature for the worker queue is identical (which it should be ... )

	// Read all relevant files and kick off work to the workers.
	if gcloud.IsBucketPath(CLI.inputDir) {
		bp, err := gcloud.NewBucketPath(CLI.inputDir)
		if err != nil {
			return err
		}

		wl.Status("Using google cloud APIs to access bucket: %s", bp.Bucket)
		bm, err := gcloud.GetBucketManager(bp.Bucket)
		if err != nil {
			return err
		}

		wl.Status("Using subpath %s", bp.Subpath)
		it, err := bm.GetBucketIterator(bp.Subpath)
		if err != nil {
			return err
		}

		c := 0
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			} else {
				if err != nil {
					return err
				}
			}

			fp := "gs://" + strings.Join([]string{bp.Bucket, attrs.Name}, string(filepath.Separator))
			queueFileForWorker(fileq, fp)

			c += 1
			wl.Status("found %d files so far ...", c)
		}
	} else {
		wl.Status("Building file list ... (this can take a while on large mounted dirs) ...")
		err := filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				wl.Status("Warning found error walking dir. Error: %s", err.Error())
			} else {
				queueFileForWorker(fileq, fp)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Done feeding work, file queue should be closed.
	close(fileq)

	// Wait for all the records to finish getting written.
	wg.Wait()

	return nil
}

////////////////////////////////////////////////////////////////////////////////
