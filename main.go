////////////////////////////////////////////////////////////////////////////////

// Sorten is a tool used to convert large batches of images from one format to
// another as needed by tensorflow-based learning systems.
package main

////////////////////////////////////////////////////////////////////////////////

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		inputDir       string
		outputDir      string
		magickBins     string
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
func hdrToJpegWorker(workerId int, jobs <-chan *hdrToJpegJob, wg *sync.WaitGroup) {
	for job := range jobs {
		cmd := exec.Command(path.Join(CLI.magickBins, "convert"), job.source, job.destination)
		if _, err := cmd.CombinedOutput(); err != nil {
			log.Printf("Warning: `convert %s %s` had error: %s", job.source, job.destination, err.Error())
		}
		wg.Done()
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
	log.Printf("Using %d CPU cores ...\n", runtime.NumCPU())

	// primitive to wait on all jobs finishing before terminating the app.
	var wg sync.WaitGroup

	// Get all files we care about ...
	files := []string{}
	fatalOnErr(filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Warning found error walking dir. Error: %s\n", err.Error())
		} else {
			if strings.ToLower(path.Ext(fp)) == ".hdr" {
				files = append(files, fp)
			}
		}
		return nil
	}))

	// Create a buffered channel to feed work, length is worst case size.
	jobs := make(chan *hdrToJpegJob, len(files))

	// Create the workers based on how many CPU cores the system has.
	for wId := 0; wId < runtime.NumCPU(); wId++ {
		go hdrToJpegWorker(wId, jobs, &wg)
	}

	// Wait-group worst case depth
	wg.Add(len(files))

	// Iterate over file list and queue them for processing.
	jobCount := 0
	for _, fp := range files {
		d, fn := filepath.Dir(fp), filepath.Base(fp)
		rd, err := filepath.Rel(CLI.inputDir, d)
		if err != nil {
			log.Printf("Error getting relative path: %s\n", err.Error())
		} else {
			dp := path.Join(CLI.outputDir, rd, fn[0:len(fn)-len(filepath.Ext(fn))]+".jpeg")
			jobs <- &hdrToJpegJob{source: fp, destination: dp}
			jobCount += 1
		}
	}

	// We are done feeding work to the workers, close the jobs channel so that
	// they can end their routines.
	close(jobs)

	wg.Wait()
	log.Printf("Successfully converted %d files.\n", jobCount)
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

	if _, err := os.Stat(CLI.outputDir); os.IsNotExist(err) {
		log.Fatalf("output directory (%s) does not exist!", CLI.outputDir)
	}

	if _, err := os.Stat(path.Join(CLI.magickBins, "convert")); os.IsNotExist(err) {
		log.Fatalf("Imagemagick not correctly installed! convert utility missing!")
	}
}

////////////////////////////////////////////////////////////////////////////////
