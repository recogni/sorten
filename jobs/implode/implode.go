////////////////////////////////////////////////////////////////////////////////

// Package implode wraps all needed logic to run a parallel job that will take
// a directory of tf records and implode them from their respective classes.
package implode

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/iterator"

	"github.com/recogni/sorten/gcloud"
	"github.com/recogni/sorten/jobs"
	"github.com/recogni/sorten/logger"

	// "github.com/recogni/tfutils"
	tf "tensorflow/core/example"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		// Command line args common to all commands
		inputDir  string
		outputDir string
		filter    string
		numShards int
		maxCount  int

		// Private stuff
		numWorkers int
		classes    []int
	}{}
)

////////////////////////////////////////////////////////////////////////////////

type fileList struct {
	front []string // buffer of unused files to pick from
	back  []string // back buffer of files that have been used so far
	popct int      // number of times pop has been called on this list
}

func newFileList() *fileList {
	return &fileList{
		front: []string{},
		back:  []string{},
		popct: 0,
	}
}

func (fl *fileList) Append(f string) {
	fl.front = append(fl.front, f)
}

func (fl *fileList) PopRandom() string {
	if len(fl.front) == 0 && len(fl.back) == 0 {
		return ""
	}

	if len(fl.front) == 0 {
		fmt.Printf("Swapping lists...\n")
		fl.front = fl.back[:len(fl.back)]
		fl.back = []string{}
	}

	x := rand.Intn(len(fl.front))
	s := fl.front[x]
	fl.back = append(fl.back, s)
	fl.front = append(fl.front[:x], fl.front[x+1:]...)
	fl.popct += 1
	return s
}

////////////////////////////////////////////////////////////////////////////////

// implodeMapper keeps track of the number of class directories and for each one
// stores a list of not-yet used files.  Once either a directory is empty or if
// the max count is hit for a given class, it will be removed from the map.
type implodeMapper struct {
	*sync.RWMutex
	wl *logger.WorkerLogger
	m  map[int]*fileList
}

func newImplodeMapper(wl *logger.WorkerLogger) (*implodeMapper, error) {
	return &implodeMapper{
		RWMutex: &sync.RWMutex{},
		wl:      wl,
		m:       map[int]*fileList{},
	}, nil
}

func (im *implodeMapper) DumpMap() {
	// im.RLock()
	// defer im.RUnlock()

	// for k, fl := range im.m {
	// 	fmt.Printf("Found key: %d with %d items:\n", k, len(fl.front))
	// 	for i := 0; i < 200; i++ {
	// 		fmt.Printf("  %d -- pop'd: %s\n", i, fl.PopRandom())
	// 	}
	// 	fmt.Printf("  Total pop count: %d\n", fl.popct)
	// }
}

func (im *implodeMapper) AddFile(outDir, inDir, filePath string) error {
	if strings.HasPrefix(filePath, inDir) {
		subPath := filePath[len(inDir):]
		if strings.HasPrefix(subPath, "/") {
			subPath = subPath[1:]
		}
		items := strings.Split(subPath, "/")
		if len(items) == 2 {
			class, _ := items[0], items[1]
			if strings.HasPrefix(class, "class_") {
				class = class[len("class_"):]
			}

			cv, err := strconv.ParseInt(class, 10, 32)
			if err != nil {
				return err
			}

			im.Lock()
			defer im.Unlock()

			if _, ok := im.m[int(cv)]; !ok {
				im.m[int(cv)] = newFileList()
			}
			im.m[int(cv)].Append(filePath)
		}
	}

	// No-op - filePath always should begin with inDir
	return fmt.Errorf("%s is an invalid input file path", filePath)
}

////////////////////////////////////////////////////////////////////////////////

func emitTfRecord(workerId int, file string, class int, rec *tf.Features, wl *logger.WorkerLogger) error {
	wl.Log(workerId, "Got emit record for class %d (%s)\n", class, file)
	return nil
}

func implodeTfWorker(workerId int, q <-chan string, wg *sync.WaitGroup, wl *logger.WorkerLogger) {
	wg.Add(1)

	// Tag the wait group since we are a new worker ready to go!
	wl.Log(workerId, "is ready for work!")
	// for {
	time.Sleep(10 * time.Second)
	// }

	// File queue is exhausted, we are done!
	wl.Log(workerId, "Worker is now done")
	wg.Done()
}

func queueFileForWorker(im *implodeMapper, q chan<- string, fp string) {
	im.AddFile(CLI.outputDir, CLI.inputDir, fp)
	// if strings.ToLower(path.Ext(fp)) == ".tfrecord" {
	// 	q <- fp
	// }
}

////////////////////////////////////////////////////////////////////////////////

func RunJob(nworkers int, args []string, wl *logger.WorkerLogger) error {
	// Setup args to parse.
	fs := flag.NewFlagSet("ImplodeTf", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to write images to")
	fs.StringVar(&CLI.filter, "filter", "", "specify classes to include, empty => include all")
	fs.IntVar(&CLI.numShards, "num-shards", 1, "number of records per .tfrecord file")
	fs.IntVar(&CLI.maxCount, "max-count", 0, "maximum number of images per class, 0 => include all")
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

	// Make an implode mapper.
	im, err := newImplodeMapper(wl)
	if err != nil {
		return err
	}

	// Create workers, each one is responsible for decrementing his waitgroup
	// after it has exhausted the work queue.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		go implodeTfWorker(wId, fileq, &wg, wl)
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
			queueFileForWorker(im, fileq, fp)

			c += 1
			wl.Status("found %d files so far ...", c)
		}
	} else {
		wl.Status("Building file list ... (this can take a while on large mounted dirs) ...")
		err := filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				wl.Status("Warning found error walking dir. Error: %s", err.Error())
			} else {
				queueFileForWorker(im, fileq, fp)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	im.DumpMap()

	// Done feeding work, file queue should be closed.
	close(fileq)

	// Wait for all the records to finish getting written.
	wg.Wait()

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	rand.Seed(time.Now().Unix())
}

////////////////////////////////////////////////////////////////////////////////
