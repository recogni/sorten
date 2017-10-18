////////////////////////////////////////////////////////////////////////////////

package main

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/recogni/sorten/logger"

	"google.golang.org/api/iterator"
)

////////////////////////////////////////////////////////////////////////////////

// hdrToJpegJob represents a source file path and the desired destination
// location after conversion.
type hdrToJpegJob struct {
	source      string
	destination string
}

// hdrToJpegWorker is a specialized worker which will run imagemagick on one
// goroutine to convert the source file into the appropriate destination
// file / format (as specified by the destination file's extension).
func hdrToJpegWorker(workerId int, jobs <-chan *hdrToJpegJob, wg *sync.WaitGroup, wl *logger.WorkerLogger) {
	wl.Log(workerId, "is ready for work")
	for job := range jobs {
		source := job.source
		if isBucketPath(job.source) {
			source = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_input.hdr", workerId))
			bp, _ := newBucketPath(job.source)
			bm := getBucketManager(bp.bucket)

			wl.Log(workerId, "attempting to download %s -> %s", job.source, source)
			if err := bm.bucketDownloadFile(source, bp); err != nil {
				wl.Log(workerId, "Warning: unable to download %s from bucket, error: %s", job.source, err.Error())
			} else {
				wl.Log(workerId, "download successful!")
			}
		}

		isBucketDst := false
		destination := job.destination
		if isBucketPath(job.destination) {
			isBucketDst = true
			destination = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_output.jpeg", workerId))
		}

		wl.Log(workerId, "converting %s -> %s", source, destination)
		cmd := exec.Command(path.Join(CLI.magickBins, "convert"), source, destination)
		if _, err := cmd.CombinedOutput(); err != nil {
			wl.Log(workerId, "Warning: `convert %s %s` had error: %s", source, destination, err.Error())
		} else {
			wl.Log(workerId, "conversion successful!")
		}

		if isBucketDst {
			wl.Log(workerId, "uploading %s to google bucket %s", destination, job.destination)
			bp, _ := newBucketPath(job.destination)
			bm := getBucketManager(bp.bucket)

			if err := bm.bucketUploadFile(bp, destination); err != nil {
				wl.Log(workerId, "Error: %s\n", err.Error())
			}
		}

		// Job done, tag the workgroup and check for more work.
		wg.Done()
		wl.Log(workerId, "is now idle")
	}
}

// queuqHdrToJpegJobFiltered checks the given file for the appropriate extension, and if
// it matches queues the current file as work for the next available worker queue.
func queuqHdrToJpegJobFiltered(fp string, jobs chan *hdrToJpegJob, wg *sync.WaitGroup) {
	if strings.ToLower(path.Ext(fp)) == ".hdr" {
		d, fn := filepath.Dir(fp), filepath.Base(fp)
		rd, err := filepath.Rel(CLI.inputDir, d)
		if err != nil {
			// TODO: Handle error case if we can open a file!
		} else {
			dstfile := fn[0:len(fn)-len(filepath.Ext(fn))] + ".jpeg"
			dpitems := []string{CLI.outputDir}
			if rd != "." {
				dpitems = append(dpitems, []string{rd, dstfile}...)
			} else {
				dpitems = append(dpitems, dstfile)
			}
			dp := strings.Join(dpitems, string(filepath.Separator))

			wg.Add(1)
			jobs <- &hdrToJpegJob{source: fp, destination: dp}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////

func RunHdrToJpegJob(nworkers int, args []string, wl *logger.WorkerLogger) error {
	var wg sync.WaitGroup

	// Parse arguments for this sub-command.
	fs := flag.NewFlagSet("HdrToJpeg", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to read images from")
	fs.StringVar(&CLI.magickBins, "magic", "/usr/local/bin/", "path to imagemagick binaries")
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
	if _, err := os.Stat(path.Join(CLI.magickBins, "convert")); os.IsNotExist(err) {
		errors.New("Imagemagick not correctly installed! convert utility missing!")
	}
	CLI.numWorkers = nworkers

	// Create a channel for the variable number of jobs to run.
	jobs := make(chan *hdrToJpegJob)

	// Create the workers based on how many CPU cores the system has.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		go hdrToJpegWorker(wId, jobs, &wg, wl)
	}

	if isBucketPath(CLI.inputDir) {
		bp, err := newBucketPath(CLI.inputDir)
		fatalOnErr(err)

		wl.Status("Using google cloud APIs to access bucket: %s", bp.bucket)
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
			queuqHdrToJpegJobFiltered(fp, jobs, &wg)

			c += 1
			wl.Status("found %d files so far ...", c)
		}

	} else {
		wl.Status("Building file list ... (this can take a while on large mounted dirs) ...")
		fatalOnErr(filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				wl.Status("Warning found error walking dir. Error: %s", err.Error())
			} else {
				queuqHdrToJpegJobFiltered(fp, jobs, &wg)
			}
			return nil
		}))
	}

	// We are done feeding work to the workers, close the jobs channel so that
	// they can end their routines.
	close(jobs)

	// Wait for all the wait groups to finish their work.
	wg.Wait()

	fmt.Printf("jobs closed")

	return nil
}

////////////////////////////////////////////////////////////////////////////////
