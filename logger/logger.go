////////////////////////////////////////////////////////////////////////////////

package logger

////////////////////////////////////////////////////////////////////////////////

import (
	"fmt"

	"github.com/buger/goterm"
)

////////////////////////////////////////////////////////////////////////////////

const (
	disableGoTerm = false
)

////////////////////////////////////////////////////////////////////////////////

// workerMsg represents the worker index as well as the message payload sent
// by said worker.
type workerMsg struct {
	index int
	msg   string
}

////////////////////////////////////////////////////////////////////////////////

// WorkerLogger is a logger which reports updates on a per-worker basis.
type WorkerLogger struct {
	nw     int // number of workers
	msgCh  chan *workerMsg
	hdrCh  chan string
	errCh  chan error
	doneCh chan struct{}
}

func New(numWorkers int) (*WorkerLogger, error) {
	return &WorkerLogger{
		nw:     numWorkers,
		msgCh:  make(chan *workerMsg, numWorkers),
		hdrCh:  make(chan string),
		errCh:  make(chan error),
		doneCh: make(chan struct{}),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (wl *WorkerLogger) Log(i int, format string, a ...interface{}) {
	if i < wl.nw {
		wl.msgCh <- &workerMsg{index: i, msg: fmt.Sprintf(format, a...)}
	}
}

func (wl *WorkerLogger) Status(format string, a ...interface{}) {
	wl.hdrCh <- fmt.Sprintf(format, a...)
}

func (wl *WorkerLogger) Error(err error) {
	// wl.errCh <- err
}

func (wl *WorkerLogger) Close() {
	wl.doneCh <- struct{}{}
}

func (wl *WorkerLogger) Start() {
	hdr := ""
	wms := make([]string, wl.nw)

	repaint := func() {
		goterm.Clear()
		defer goterm.Flush()

		goterm.MoveCursor(1, 1)
		goterm.Printf("%s\n", hdr)
		for i := 0; i < wl.nw; i++ {
			goterm.MoveCursor(1, i+2)
			goterm.Printf("worker %02d :: %s\n", i, wms[i])
		}
	}

	for {
		select {
		case newHdr := <-wl.hdrCh:
			hdr = newHdr
			if disableGoTerm {
				fmt.Println(hdr)
			}
		case wMsg := <-wl.msgCh:
			wms[wMsg.index] = wMsg.msg
			if disableGoTerm {
				fmt.Println(wMsg.msg)
			}
		case <-wl.doneCh:
			return
		case err := <-wl.errCh:
			hdr = fmt.Sprintf("Error: %s", err.Error())
		}

		if !disableGoTerm {
			repaint()
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
