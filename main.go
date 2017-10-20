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

	"github.com/recogni/sorten/jobs/explode"
	"github.com/recogni/sorten/jobs/h2j"
	"github.com/recogni/sorten/jobs/implode"
	"github.com/recogni/sorten/jobs/j2tf"
)

////////////////////////////////////////////////////////////////////////////////

var (
	numWorkers = runtime.NumCPU()
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
	wl, err := logger.New(numWorkers)
	fatalOnErr(err)
	go wl.Start()
	defer wl.Close()

	cmd, args := os.Args[1], os.Args[2:]
	switch strings.ToLower(cmd) {
	case "h2j":
		err = h2j.RunJob(numWorkers, args, wl)
	case "j2tf":
		err = j2tf.RunJob(numWorkers, args, wl)
	case "explode", "exp", "split":
		err = explode.RunJob(numWorkers, args, wl)
	case "implode", "imp", "merge":
		err = implode.RunJob(numWorkers, args, wl)
	default:
		err = fmt.Errorf("unknown command %s", cmd)
	}
	if err != nil {
		wl.Error(err)
	}
	fatalOnErr(err)
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	log.SetPrefix("")
	log.SetFlags(0)
	log.SetOutput(os.Stdout)
}

////////////////////////////////////////////////////////////////////////////////
