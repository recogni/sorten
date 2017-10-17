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
	"sync"

	"github.com/burger/goterm"
)

////////////////////////////////////////////////////////////////////////////////

var (
	numWorkers = 1
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
	setUpdate(updates, numWorkers, format, a...)
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

	// Make a channel to get updates from the workers so we can aggregate
	// messages nicelyer.
	updates := make(chan *workerUpdate)
	go repaintUpdates(numWorkers, updates)
	setStatus(updates, "Using %d CPU cores ...", numWorkers)

	if len(os.Args) < 2 {
		log.Fatalf("No command specified! Valid commands include: [h2j, j2tf]\n")
	}

	var wg sync.WaitGroup
	{
		var err error
		cmd, args := os.Args[1], os.Args[2:]
		switch strings.ToLower(cmd) {
		case "h2j":
			err = RunHdrToJpegJob(numWorkers, args, updates, &wg)
		default:
			err = fmt.Errorf("unknown command %s", cmd)
		}
		fatalOnErr(err)
	}

	wg.Wait()

	setStatus(updates, "Done!")
	close(updates)
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	log.SetPrefix("")
	log.SetFlags(0)
	log.SetOutput(os.Stdout)

	// TODO: Override from env / command line flag.
	numWorkers = runtime.NumCPU()
}

////////////////////////////////////////////////////////////////////////////////
