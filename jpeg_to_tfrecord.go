////////////////////////////////////////////////////////////////////////////////

package main

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/iterator"
)

////////////////////////////////////////////////////////////////////////////////

type jpegToTfRecordNameFactory struct {
	sync.RWMutex
	recordCount int
}

func (jf *jpegToTfRecordNameFactory) GetNextTfRecordName(prefix string) string {
	jf.Lock()
	defer func() {
		jf.recordCount += 1
		jf.Unlock()
	}()

	return fmt.Sprintf("%s%05d.tfrecord", prefix, jf.recordCount)
}

// Global instance that is go-routine safe to access.  This is used
// by the various workers to fetch the appropriate name for the next
// record that they might want to write.  It is expected that a worker
// will only ask for a name once he finds at-least one record to write.
var j2tfFactory = &jpegToTfRecordNameFactory{recordCount: 0}

////////////////////////////////////////////////////////////////////////////////

// jpegToTfRecordStatus keeps track of a single workers status for a
// given running job.
type jpegToTfRecordStatus struct {
	workerId int      // id of the worker that made this file
	tfdst    string   // output record file name
	count    int      // number of records that we have seen so far
	files    []string // files that we have converted so far
}

func newJpegToTfRecordStatus(wId int, dst string) *jpegToTfRecordStatus {
	return &jpegToTfRecordStatus{
		workerId: wId,
		tfdst:    dst,
		count:    0,
		files:    []string{},
	}
}

func (js *jpegToTfRecordStatus) AddFile(file string) {
	js.count += 1
	js.files = append(js.files, file)
}

func (js *jpegToTfRecordStatus) Marshal() []byte {
	ret := fmt.Sprintf(`# Generated meta-data file
Worker ID:     %d
Record File:   %s
Num Files:     %d
File List:
`, js.workerId, js.tfdst, js.count)
	for _, f := range js.files {
		ret += f + "\n"
	}
	return []byte(ret)
}

func (js *jpegToTfRecordStatus) Flush() {
	bs := js.Marshal()
	fmt.Printf("%s\n", string(bs))
}

////////////////////////////////////////////////////////////////////////////////

// jpegToTfRecordWorker is ...
func jpegToTfRecordWorker(workerId int, fileq <-chan string, updates chan<- *workerUpdate, wg *sync.WaitGroup) {
	setUpdate(updates, workerId, "is ready for work (records per tf=%d)", CLI.filesPerRecord)
	time.Sleep(1 * time.Second)

	var status *jpegToTfRecordStatus
	for file := range fileq {
		// If the status is nil, and we have a file to process, grab
		// a file name from the factory and make a new status variable.
		if status == nil {
			fname := j2tfFactory.GetNextTfRecordName(CLI.recordPrefix)
			dstfp := strings.Join([]string{CLI.outputDir, fname}, string(filepath.Separator))
			status = newJpegToTfRecordStatus(workerId, dstfp)

			setUpdate(updates, workerId, "Got a new record (%s)", status.tfdst)
			time.Sleep(1 * time.Second)
		}

		setUpdate(updates, workerId, "got file from queue: %s", file)

		// TODO: Add stuff to the writer!
		time.Sleep(1 * time.Second)

		// Once we have added this file to the record, tag it in the status.
		status.AddFile(file)

		// If the status count has matched the number of nodes per shard,
		// we should finish up writing using the current writer and set
		// the status to nil so we grab a new file to write.
		if status.count == CLI.filesPerRecord {
			status.Flush()
			status = nil

			setUpdate(updates, workerId, "Flushing status! (%s)", status.tfdst)
			time.Sleep(1 * time.Second)
		}

		// Job done, tag the workgroup and check for more work.
		setUpdate(updates, workerId, "is now idle")
	}

	// If we are done with files to process, we need to close the writer
	// in the current status (if any), and proceed to tag the wait group
	// as complete.
	if status != nil {
		status.Flush()
	}
	wg.Done()
}

// queueFileForJpegToTfRecordJob is called on all files that are found by the
// appropriate file walker.  If the file's extension matches what we are looking
// for, it will queue it to the buffered channel of files to translate into a
// TFRecord.
func queueFileForJpegToTfRecordJob(fileq chan string, fp string) {
	if strings.ToLower(path.Ext(fp)) == ".jpeg" {
		fileq <- fp
	}
}

////////////////////////////////////////////////////////////////////////////////

func RunJpegToTFRecordJob(nworkers int, args []string, updates chan<- *workerUpdate) error {
	var wg sync.WaitGroup

	// Parse arguments for this sub-command.
	fs := flag.NewFlagSet("JpegToTfRecord", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to read images from")
	fs.StringVar(&CLI.recordPrefix, "prefix", "", "tf record name prefix")
	fs.IntVar(&CLI.filesPerRecord, "shard-size", 1, "number of records per shard")

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
	CLI.numWorkers = nworkers

	// Create a queue of files that we can keep ready for the worker to consume
	// since each worker will be aggregating a bunch of files.  Each job will
	// be responsible for creating its own output file in the output directory
	// and there will be a global mutex which controls the allowable file names.
	// Once all files have been loaded, or when the job count is hit for a job
	// worker, it is responsible for closing the record writer and asking for a
	// new output file provided there is more work to do.
	fileq := make(chan string, CLI.numWorkers*4)

	// Create the workers based on how many CPU cores the system has.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		wg.Add(1)
		go jpegToTfRecordWorker(wId, fileq, updates, &wg)
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
			queueFileForJpegToTfRecordJob(fileq, fp)

			c += 1
			setStatus(updates, "found %d files so far ...", c)
		}

	} else {
		setStatus(updates, "Building file list ... (this can take a while on large mounted dirs) ...")
		fatalOnErr(filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				setStatus(updates, "Warning found error walking dir. Error: %s", err.Error())
			} else {
				queueFileForJpegToTfRecordJob(fileq, fp)
			}
			return nil
		}))
	}

	// We are done feeding work to the workers, close the fileq channel so that
	// they can end their routines.
	close(fileq)

	// Wait for all the records to finish getting written.
	wg.Wait()

	return nil
}

////////////////////////////////////////////////////////////////////////////////
