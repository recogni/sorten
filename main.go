////////////////////////////////////////////////////////////////////////////////

// Sorten is a tool used to convert large batches of images from one format to
// another as needed by tensorflow-based learning systems.
package main

////////////////////////////////////////////////////////////////////////////////

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/burger/goterm"
	"google.golang.org/api/iterator"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		inputDir       string
		outputDir      string
		magickBins     string
		useImageMagick bool
		numCpus        int
	}{}

	gDebugPrintsEnabled = true
)

////////////////////////////////////////////////////////////////////////////////

// workerUpdate contains a worker index and a message being sent from the
// worker to the main thread.
type workerUpdate struct {
	id  int
	msg string
}

// repaintUpdates waits on all specified workers and repaints the progress
// string per worker to stdout.
func repaintUpdates(workerCount int, updates <-chan *workerUpdate) {
	header := "Application starting up ..."
	us := make([]string, workerCount)
	for i := 0; i < workerCount; i++ {
		us[i] = fmt.Sprintf("initializing ...")
	}

	repaintCounter := 0
	repaint := func() {
		goterm.Clear()
		defer goterm.Flush()

		goterm.MoveCursor(1, 1)
		goterm.Printf("%s\n", header)
		for i := 0; i < workerCount; i++ {
			goterm.MoveCursor(1, i+2)
			goterm.Printf("worker %02d :: %s\n", i, us[i])
		}
		repaintCounter += 1
	}

	repaint()
	for update := range updates {
		if update.id == workerCount {
			header = update.msg
		} else if update.id >= 0 && update.id < workerCount {
			us[update.id] = update.msg
		}
		repaint()
		// time.Sleep(32 * time.Millisecond)
	}
}

func setUpdate(updates chan<- *workerUpdate, index int, format string, a ...interface{}) {
	updates <- &workerUpdate{
		id:  index,
		msg: fmt.Sprintf(format, a...),
	}
}

func setStatus(updates chan<- *workerUpdate, format string, a ...interface{}) {
	setUpdate(updates, CLI.numCpus, format, a...)
}

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
func hdrToJpegWorker(workerId int, jobs <-chan *hdrToJpegJob, updates chan<- *workerUpdate, wg *sync.WaitGroup) {
	setUpdate(updates, workerId, "is ready for work")
	for job := range jobs {
		source := job.source
		if isBucketPath(job.source) {
			source = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_input.hdr", workerId))
			bp, _ := newBucketPath(job.source)
			bm := getBucketManager(bp.bucket)

			setUpdate(updates, workerId, "attempting to download %s -> %s", job.source, source)
			if err := bm.bucketDownload(source, bp); err != nil {
				setUpdate(updates, workerId, "Warning: unable to download %s from bucket, error: %s", job.source, err.Error())
			} else {
				setUpdate(updates, workerId, "download successful!")
			}
		}

		isBucketDst := false
		destination := job.destination
		if isBucketPath(job.destination) {
			isBucketDst = true
			destination = path.Join(os.TempDir(), fmt.Sprintf("worker_%d_output.jpeg", workerId))
		}

		setUpdate(updates, workerId, "converting %s -> %s", source, destination)
		cmd := exec.Command(path.Join(CLI.magickBins, "convert"), source, destination)
		if _, err := cmd.CombinedOutput(); err != nil {
			setUpdate(updates, workerId, "Warning: `convert %s %s` had error: %s", source, destination, err.Error())
		} else {
			setUpdate(updates, workerId, "conversion successful!")
		}

		if isBucketDst {
			setUpdate(updates, workerId, "uploading %s to google bucket %s", destination, job.destination)
			bp, _ := newBucketPath(job.destination)
			bm := getBucketManager(bp.bucket)

			if err := bm.bucketUpload(bp, destination); err != nil {
				setUpdate(updates, workerId, "Error: %s\n", err.Error())
			} else if gDebugPrintsEnabled {
				setUpdate(updates, workerId, "upload successful!")
			}
		}

		// Job done, tag the workgroup and check for more work.
		wg.Done()
		setUpdate(updates, workerId, "is now idle")
	}
}

// queueJobFiltered checks the given file for the appropriate extension, and if
// it matches queues the current file as work for the next available worker queue.
func queuqJobFiltered(fp string, jobs chan *hdrToJpegJob, wg *sync.WaitGroup) {
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

// fatalOnErr will abort the app on any fatal errors.
func fatalOnErr(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

// main entry point.
func main() {
	var wg sync.WaitGroup
	jobs := make(chan *hdrToJpegJob)

	// Make a channel to get updates from the workers so we can aggregate
	// messages nicelyer.
	updates := make(chan *workerUpdate)
	go repaintUpdates(CLI.numCpus, updates)

	setStatus(updates, "Using %d CPU cores ...", CLI.numCpus)

	// Create the workers based on how many CPU cores the system has.
	for wId := 0; wId < CLI.numCpus; wId++ {
		go hdrToJpegWorker(wId, jobs, updates, &wg)
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
			queuqJobFiltered(fp, jobs, &wg)

			c += 1
			setStatus(updates, "found %d files so far ...", c)
		}

	} else {
		setStatus(updates, "Building file list ... (this can take a while on large mounted dirs) ...")
		fatalOnErr(filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				setStatus(updates, "Warning found error walking dir. Error: %s", err.Error())
			} else {
				queuqJobFiltered(fp, jobs, &wg)
			}
			return nil
		}))
	}

	// We are done feeding work to the workers, close the jobs channel so that
	// they can end their routines.
	close(jobs)

	wg.Wait()

	setStatus(updates, "Done!")
	close(updates)
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	log.SetPrefix("")
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	flag.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	flag.StringVar(&CLI.outputDir, "output", "", "output directory to read images from")
	flag.StringVar(&CLI.magickBins, "magic", "/usr/local/bin/", "path to imagemagick binaries")
	flag.Parse()

	if len(CLI.inputDir) == 0 {
		log.Fatalf("specify input directory with --input")
	}

	if len(CLI.outputDir) == 0 {
		log.Fatalf("specify output directory with --output")
	}

	if !isBucketPath(CLI.outputDir) {
		if _, err := os.Stat(CLI.outputDir); os.IsNotExist(err) {
			log.Fatalf("output directory (%s) does not exist!", CLI.outputDir)
		}
	}

	if _, err := os.Stat(path.Join(CLI.magickBins, "convert")); os.IsNotExist(err) {
		log.Fatalf("Imagemagick not correctly installed! convert utility missing!")
	}

	CLI.numCpus = runtime.NumCPU()
}

////////////////////////////////////////////////////////////////////////////////
