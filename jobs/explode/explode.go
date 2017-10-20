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
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/recogni/sorten/gcloud"
	"github.com/recogni/sorten/jobs"
	"github.com/recogni/sorten/logger"

	"google.golang.org/api/iterator"
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

func explodeTfWorker(workerId int, q <-chan string, wg *sync.WaitGroup, wl *logger.WorkerLogger) {
	// Tag the wait group since we are a new worker ready to go!
	wg.Add(1)

	wl.Log(workerId, "is ready for work!")
	for file := range q {
		wl.Log(workerId, "GOT FILE: %s", file)
		time.Sleep(1 * time.Second)
	}

	// File queue is exhausted, we are done!
	wg.Done()
}

func queueFileForWorker(q chan<- string, fp string) {
	if strings.ToLower(path.Ext(fp)) == ".tfrecord" {
		q <- fp
	}
}

////////////////////////////////////////////////////////////////////////////////

func RunJob(nworkers int, args []string, wl *logger.WorkerLogger) error {
	// Setup args to parse.
	fs := flag.NewFlagSet("ExplodeTf", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to write images to")
	fs.StringVar(&CLI.filter, "filter", "", "specify classes to include, empty == include all")

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

	return nil
}

////////////////////////////////////////////////////////////////////////////////
