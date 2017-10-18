////////////////////////////////////////////////////////////////////////////////

// Sorten is a tool used to convert large batches of images from one format to
// another as needed by tensorflow-based learning systems.
package main

////////////////////////////////////////////////////////////////////////////////

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/recogni/sorten/logger"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		// Command line args common to all commands
		inputDir       string
		outputDir      string
		magickBins     string
		recordPrefix   string
		filesPerRecord int
		imageClassId   int

		// Private stuff
		useImageMagick bool
		numWorkers     int
	}{}
)

////////////////////////////////////////////////////////////////////////////////

// fatalOnErr will abort the app on any fatal errors.
func fatalOnErr(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

// main entry point.
func main() {
	if len(os.Args) < 2 {
		log.Fatalf("No command specified! Valid commands include: [h2j, j2tf]\n")
	}

	// Make a logger
	wl, err := logger.New(CLI.numWorkers)
	fatalOnErr(err)
	go wl.Start()
	defer wl.Close()

	cmd, args := os.Args[1], os.Args[2:]
	switch strings.ToLower(cmd) {
	case "h2j":
		err = RunHdrToJpegJob(CLI.numWorkers, args, wl)
	case "j2tf":
		err = RunJpegToTFRecordJob(CLI.numWorkers, args, wl)
	default:
		err = fmt.Errorf("unknown command %s", cmd)
	}
	fatalOnErr(err)

	wl.Status("Done!")
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	log.SetPrefix("")
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	CLI.numWorkers = runtime.NumCPU()
}

////////////////////////////////////////////////////////////////////////////////
