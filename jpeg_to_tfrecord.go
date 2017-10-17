////////////////////////////////////////////////////////////////////////////////

package main

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/iterator"
)

////////////////////////////////////////////////////////////////////////////////

// jpegToTfRecordJob represents a source file path and the desired destination
// location after conversion.
type jpegToTfRecordJob struct {
	source      string
	destination string
}

// jpegToTfRecordWorker is ...
func jpegToTfRecordWorker(workerId int, fileq <-chan string, updates chan<- *workerUpdate, wg *sync.WaitGroup) {
	setUpdate(updates, workerId, "is ready for work (records per tf=%d)", CLI.filesPerRecord)
	time.Sleep(1 * time.Second)

	for file := range fileq {
		setUpdate(updates, workerId, "got file from queue: %s", file)
		time.Sleep(1 * time.Second)

		// Job done, tag the workgroup and check for more work.
		setUpdate(updates, workerId, "is now idle")
	}

	wg.Done()
}

// queueFileForJpegToTfRecordJob is called on all files that are found by the
// appropriate file walker.  If the file's extension matches what we are looking
// for, it will queue it to the buffered channel of files to translate into a
// TFRecord.
func queueFileForJpegToTfRecordJob(fileq chan string, fp string) {
	if strings.ToLower(path.Ext(fp)) == ".jpeg" {
		fileq <- fp
	}
}

////////////////////////////////////////////////////////////////////////////////

func RunJpegToTFRecordJob(nworkers int, args []string, updates chan<- *workerUpdate, wg *sync.WaitGroup) error {
	// Parse arguments for this sub-command.
	fs := flag.NewFlagSet("JpegToTfRecord", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to read images from")
	fs.IntVar(&CLI.filesPerRecord, "shard-size", 1, "number of records per shard")
	fs.Parse(args)

	if len(CLI.inputDir) == 0 {
		errors.New("specify input directory with --input")
	}
	if len(CLI.outputDir) == 0 {
		errors.New("specify output directory with --output")
	}
	if !isBucketPath(CLI.outputDir) {
		if _, err := os.Stat(CLI.outputDir); os.IsNotExist(err) {
			fmt.Errorf("output directory (%s) does not exist!", CLI.outputDir)
		}
	}
	CLI.numWorkers = nworkers

	// Create a queue of files that we can keep ready for the worker to consume
	// since each worker will be aggregating a bunch of files.  Each job will
	// be responsible for creating its own output file in the output directory
	// and there will be a global mutex which controls the allowable file names.
	// Once all files have been loaded, or when the job count is hit for a job
	// worker, it is responsible for closing the record writer and asking for a
	// new output file provided there is more work to do.
	fileq := make(chan string, CLI.numWorkers*4)

	// Create the workers based on how many CPU cores the system has.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		wg.Add(1)
		go jpegToTfRecordWorker(wId, fileq, updates, wg)
	}

	if isBucketPath(CLI.inputDir) {
		bp, err := newBucketPath(CLI.inputDir)
		fatalOnErr(err)

		setStatus(updates, "Using google cloud APIs to access bucket: %s", bp.bucket)
		bm := getBucketManager(bp.bucket)

		it, err := bm.getBucketIterator(bp.subpath)
		fatalOnErr(err)

		c := 0
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			} else {
				fatalOnErr(err)
			}

			fp := "gs://" + strings.Join([]string{bp.bucket, attrs.Name}, string(filepath.Separator))
			queueFileForJpegToTfRecordJob(fileq, fp)

			c += 1
			setStatus(updates, "found %d files so far ...", c)
		}

	} else {
		setStatus(updates, "Building file list ... (this can take a while on large mounted dirs) ...")
		fatalOnErr(filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				setStatus(updates, "Warning found error walking dir. Error: %s", err.Error())
			} else {
				queueFileForJpegToTfRecordJob(fileq, fp)
			}
			return nil
		}))
	}

	// We are done feeding work to the workers, close the fileq channel so that
	// they can end their routines.
	close(fileq)
	return nil
}

////////////////////////////////////////////////////////////////////////////////
