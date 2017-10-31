////////////////////////////////////////////////////////////////////////////////

package h2j

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

	"github.com/recogni/sorten/gcloud"
	"github.com/recogni/sorten/logger"

	"google.golang.org/api/iterator"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		// Command line args common to all commands
		inputDir   string
		outputDir  string
		magickBins string
		lumiBins   string
		numWorkers int

		// Private stuff
		useImageMagick bool
	}{}
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
		if gcloud.IsBucketPath(job.source) {
			source = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_input.hdr", workerId))
			bp, _ := gcloud.NewBucketPath(job.source)
			bm, _ := gcloud.GetBucketManager(bp.Bucket)

			wl.Log(workerId, "attempting to download %s -> %s", job.source, source)
			if err := bm.BucketDownloadFile(source, bp); err != nil {
				wl.Log(workerId, "Warning: unable to download %s from bucket, error: %s", job.source, err.Error())
			} else {
				wl.Log(workerId, "download successful!")
			}
		}

		isBucketDst := false
		destination := job.destination
		if gcloud.IsBucketPath(job.destination) {
			isBucketDst = true
			destination = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_output.jpeg", workerId))
		} else {
			// Local file - verify that its parent directory exists
			dir := path.Dir(destination)
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				if err := os.MkdirAll(dir, 0755); err != nil {
					// TODO: This should be done from a mutex :)
					// TODO: Ignore this error for now
					wl.Error(err)
				}
			}
		}

		// // 1. Convert from HDR -> TIFF using luminance-hdr-cli
		// tempFile := path.Join(os.TempDir(), fmt.Sprintf("worker_%d_temporary.tiff", workerId))
		// wl.Log(workerId, "converting HDR -> TIFF %s -> %s", source, tempFile)
		// cmd0 := exec.Command(path.Join(CLI.lumiBins, "luminance-hdr-cli"), "-l", source, "-t", "durand", "-o", tempFile)
		// if _, err := cmd0.CombinedOutput(); err != nil {
		// 	wl.Log(workerId, "Warning: `luminance-hdr-cli %s %s` had error: %s", source, tempFile, err.Error())
		// } else {
		// 	wl.Log(workerId, "conversion successful!")
		// }

		// 2. Convert from TIFF -> JPEG using image-magick
		// wl.Log(workerId, "converting TIFF -> JPEG %s -> %s", tempFile, destination)

		// Convert from HDR -> JPEG
		cmd1 := exec.Command(path.Join(CLI.magickBins, "convert"), source /*"-quality", "100",*/, "-colorspace", "RGB", destination)
		if _, err := cmd1.CombinedOutput(); err != nil {
			wl.Log(workerId, "Warning: `convert %s %s` had error: %s", source, destination, err.Error())
		} else {
			wl.Log(workerId, "conversion successful!")
		}

		if isBucketDst {
			wl.Log(workerId, "uploading %s to google bucket %s", destination, job.destination)
			bp, _ := gcloud.NewBucketPath(job.destination)
			bm, _ := gcloud.GetBucketManager(bp.Bucket)

			if err := bm.BucketUploadFile(bp, destination); err != nil {
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
	fmt.Printf("Found file: %s", fp)
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

func RunJob(nworkers int, args []string, wl *logger.WorkerLogger) error {
	var wg sync.WaitGroup

	// Parse arguments for this sub-command.
	fs := flag.NewFlagSet("HdrToJpeg", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to read images from")
	fs.StringVar(&CLI.magickBins, "magic", "/usr/local/bin/", "path to imagemagick binaries")
	fs.StringVar(&CLI.lumiBins, "luminance", "/usr/local/bin/", "path to luminance-hdr binaries")
	fs.IntVar(&CLI.numWorkers, "workers", nworkers, "number of workers to use (default == num cpus)")
	fs.Parse(args)

	if len(CLI.inputDir) == 0 {
		errors.New("specify input directory with --input")
	}
	if len(CLI.outputDir) == 0 {
		errors.New("specify output directory with --output")
	}
	if !gcloud.IsBucketPath(CLI.outputDir) {
		if _, err := os.Stat(CLI.outputDir); os.IsNotExist(err) {
			if err := os.MkdirAll(CLI.outputDir, 0755); err != nil {
				return err
			}
		}
	}
	if _, err := os.Stat(path.Join(CLI.magickBins, "convert")); os.IsNotExist(err) {
		return errors.New("Imagemagick not correctly installed! convert utility missing!")
	}
	// if _, err := os.Stat(path.Join(CLI.lumiBins, "luminance-hdr-cli")); os.IsNotExist(err) {
	// 	return errors.New("Luminance-hdr not correctly installed! luminance-hdr-cli missing!")
	// }

	// Create a channel for the variable number of jobs to run.
	jobs := make(chan *hdrToJpegJob)

	// Create the workers based on how many CPU cores the system has.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		go hdrToJpegWorker(wId, jobs, &wg, wl)
	}

	// TODO: SHABA_BOILERPLATE_ITERATOR
	// The below code is SUPER boilerplate! and can be abstracted out fi the
	// signature for the worker queue is identical (which it should be ... )
	if gcloud.IsBucketPath(CLI.inputDir) {
		bp, err := gcloud.NewBucketPath(CLI.inputDir)
		if err != nil {
			return err
		}

		wl.Status("Using google cloud APIs to access bucket: %s", bp.Bucket)
		bm, err := gcloud.GetBucketManager(bp.Bucket)

		it, err := bm.GetBucketIterator(bp.Subpath)
		if err != nil {
			return err
		}

		c := 0
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				wl.Status("Warning found error: %s", err.Error())
			}

			fp := "gs://" + strings.Join([]string{bp.Bucket, attrs.Name}, string(filepath.Separator))
			queuqHdrToJpegJobFiltered(fp, jobs, &wg)

			c += 1
			wl.Status("found %d files so far ...", c)
		}

	} else {
		wl.Status("Building file list ... (this can take a while on large mounted dirs) ...")
		err := (filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				wl.Status("Warning found error walking dir. Error: %s", err.Error())
			} else {
				queuqHdrToJpegJobFiltered(fp, jobs, &wg)
			}
			return nil
		}))
		if err != nil {
			return err
		}
	}

	// We are done feeding work to the workers, close the jobs channel so that
	// they can end their routines.
	close(jobs)

	// Wait for all the wait groups to finish their work.
	wg.Wait()

	return nil
}

////////////////////////////////////////////////////////////////////////////////
