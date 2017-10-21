////////////////////////////////////////////////////////////////////////////////

// Package implode wraps all needed logic to run a parallel job that will take
// a directory of tf records and implode them from their respective classes.
package implode

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/api/iterator"

	"github.com/recogni/sorten/gcloud"
	"github.com/recogni/sorten/jobs"
	"github.com/recogni/sorten/logger"

	"github.com/recogni/tfutils"
	tf "tensorflow/core/example"
)

////////////////////////////////////////////////////////////////////////////////

var (
	CLI = struct {
		// Command line args common to all commands
		inputDir  string
		outputDir string
		prefix    string // output prefix
		filter    string
		shardSize int
		maxCount  int

		// Private stuff
		numWorkers int
		classes    []int
	}{}
)

////////////////////////////////////////////////////////////////////////////////

type Job struct {
	file  string
	class int
}

////////////////////////////////////////////////////////////////////////////////

type implodeFactory struct {
	sync.RWMutex
	recordCount int
}

func (imf *implodeFactory) GetNextTfRecordName(prefix string) string {
	imf.Lock()
	defer func() {
		imf.recordCount += 1
		imf.Unlock()
	}()

	return fmt.Sprintf("%s%05d", prefix, imf.recordCount)
}

// Global instance that is go-routine safe to access.  This is used
// by the various workers to fetch the appropriate name for the next
// record that they might want to write.  It is expected that a worker
// will only ask for a name once he finds at-least one record to write.
var iFactory = &implodeFactory{recordCount: 0}

////////////////////////////////////////////////////////////////////////////////

type fileList struct {
	front []string // buffer of unused files to pick from
	back  []string // back buffer of files that have been used so far
	popct int      // number of times pop has been called on this list
}

func newFileList() *fileList {
	return &fileList{
		front: []string{},
		back:  []string{},
		popct: 0,
	}
}

func (fl *fileList) Append(f string) {
	fl.front = append(fl.front, f)
}

func (fl *fileList) PopRandom() string {
	if len(fl.front) == 0 && len(fl.back) == 0 {
		return ""
	}

	if len(fl.front) == 0 {
		fl.front = fl.back[:len(fl.back)]
		fl.back = []string{}
	}

	x := rand.Intn(len(fl.front))
	s := fl.front[x]
	fl.back = append(fl.back, s)
	fl.front = append(fl.front[:x], fl.front[x+1:]...)
	fl.popct += 1
	return s
}

////////////////////////////////////////////////////////////////////////////////

// implodeTfWriter keeps track of a single workers status for a
// given running job.
type implodeTfWriter struct {
	writer     *tfutils.RecordWriter
	writerFile string   // path to the writer's file, if tfdst is local, this is tfdst
	workerId   int      // id of the worker that made this file
	tfdst      string   // output record path name
	count      int      // number of records that we have seen so far
	files      []string // files that we have converted so far
}

func newimplodeTfWriter(wId int, dst string) *implodeTfWriter {
	writerOutFile := dst
	if gcloud.IsBucketPath(dst) {
		// Create a temporary file name for this worker to write to - if it already
		// exists, nuke the file so the writer starts again.
		writerOutFile = path.Join(os.TempDir(), fmt.Sprintf("worker_%d.tfrecord", wId))
	} else {
		// Local file - verify that its parent directory exists
		dir := path.Dir(writerOutFile)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				// TODO: This should be done from a mutex :)
				// TODO: Ignore this error for now
				// return err
			}
		}
	}

	w, err := tfutils.NewWriter(writerOutFile, nil)
	if err != nil {
		panic(err)
	}

	return &implodeTfWriter{
		writer:     w,
		writerFile: writerOutFile,
		workerId:   wId,
		tfdst:      dst,
		count:      0,
		files:      []string{},
	}
}

func (iw *implodeTfWriter) Marshal() []byte {
	ret := fmt.Sprintf(`# Generated meta-data file
Worker ID:     %d
Record File:   %s
Num Files:     %d
File List:
`, iw.workerId, iw.tfdst, iw.count)
	for _, f := range iw.files {
		ret += f + "\n"
	}
	return []byte(ret)
}

func (iw *implodeTfWriter) AddFile(class int, file string) error {
	sourceFile := file
	if gcloud.IsBucketPath(file) {
		sourceFile = path.Join(os.TempDir(), fmt.Sprintf("worker_%d.tfrecord", iw.workerId))
		bp, err := gcloud.NewBucketPath(file)
		if err != nil {
			return err
		}

		bm, err := gcloud.GetBucketManager(bp.Bucket)
		if err := bm.BucketDownloadFile(sourceFile, bp); err != nil {
			return err
		}
	}

	bs, err := ioutil.ReadFile(sourceFile)
	if err != nil {
		return err
	}

	iw.writer.WriteRecord(bs)
	iw.count += 1
	iw.files = append(iw.files, file)
	return nil
}

func (iw *implodeTfWriter) Flush() error {
	if iw == nil || iw.count == 0 {
		return nil
	}

	// If the destination file is a bucket path, we upload the intermediate
	// file to the expected destination.
	metaFile := iw.tfdst + ".metadata"
	if gcloud.IsBucketPath(iw.tfdst) {
		bp, err := gcloud.NewBucketPath(iw.tfdst)
		if err != nil {
			return err
		}

		bm, err := gcloud.GetBucketManager(bp.Bucket)
		if err != nil {
			return err
		}

		err = bm.BucketUploadFile(bp, iw.writerFile)
		if err != nil {
			return err
		}

		bp, err = gcloud.NewBucketPath(metaFile)
		if err != nil {
			return err
		}
		return bm.BucketUpload(bp, iw.Marshal())
	} else {
		ioutil.WriteFile(metaFile, iw.Marshal(), 0600)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

// implodeMapper keeps track of the number of class directories and for each one
// stores a list of not-yet used files.  Once either a directory is empty or if
// the max count is hit for a given class, it will be removed from the map.
type implodeMapper struct {
	*sync.RWMutex
	initOnce *sync.Once

	wl        *logger.WorkerLogger
	m         map[int]*fileList
	keys      []int // list of keys to choose from
	shardSize int   // Number of tfrecords to pack into one imploded record
	classSize int   // Number of tfrecords to grab per class
}

func newImplodeMapper(shardSize, classSize int, wl *logger.WorkerLogger) (*implodeMapper, error) {
	return &implodeMapper{
		RWMutex:  &sync.RWMutex{},
		initOnce: &sync.Once{},

		wl:        wl,
		m:         map[int]*fileList{},
		keys:      []int{},
		shardSize: shardSize,
		classSize: classSize,
	}, nil
}

func (im *implodeMapper) Initialize() {
	im.Lock()
	defer im.Unlock()

	im.wl.Status("Implode mapper initializing keys ...")
	im.keys = []int{}
	for k, _ := range im.m {
		im.keys = append(im.keys, k)
	}
}

func (im *implodeMapper) AddFile(outDir, inDir, filePath string) error {
	if strings.HasPrefix(filePath, inDir) {
		subPath := filePath[len(inDir):]
		if strings.HasPrefix(subPath, "/") {
			subPath = subPath[1:]
		}
		items := strings.Split(subPath, "/")
		if len(items) == 2 {
			class, _ := items[0], items[1]
			if strings.HasPrefix(class, "class_") {
				class = class[len("class_"):]
			}

			cv, err := strconv.ParseInt(class, 10, 32)
			if err != nil {
				return err
			}

			im.Lock()
			defer im.Unlock()

			if _, ok := im.m[int(cv)]; !ok {
				im.m[int(cv)] = newFileList()
			}
			im.m[int(cv)].Append(filePath)
		}
	}

	// No-op - filePath always should begin with inDir
	return fmt.Errorf("%s is an invalid input file path", filePath)
}

// Drain returns one random tf record file path from the implode mapper.  If
// a class has pop'd `classSize` number of items - it will be removed from the
// mapper.
func (im *implodeMapper) Drain() (*Job, error) {
	// Initialize the keys on the first call to drain.
	im.initOnce.Do(im.Initialize)

	// Exit condition, if the keys are empty we have nothing left to pop.
	if len(im.keys) == 0 {
		return nil, io.EOF
	}

	// Grab a random key.
	idx := rand.Intn(len(im.keys))
	key := im.keys[idx]

	// Get an entry from the file list, and nuke the filelist if
	// it has pop'd enough entries.
	im.Lock()
	defer im.Unlock()

	if fl, ok := im.m[key]; ok {
		fp := fl.PopRandom()
		if fl.popct >= im.classSize {
			delete(im.m, key)
			im.keys = append(im.keys[:idx], im.keys[idx+1:]...)
		}
		return &Job{file: fp, class: key}, nil
	}
	return nil, errors.New("unexplainable errors are happening - call shaba")
}

////////////////////////////////////////////////////////////////////////////////

func emitTfRecord(workerId int, file string, class int, rec *tf.Features, wl *logger.WorkerLogger) error {
	wl.Log(workerId, "Got emit record for class %d (%s)\n", class, file)
	return nil
}

func implodeTfWorker(workerId int, q <-chan *Job, wg *sync.WaitGroup, wl *logger.WorkerLogger) {
	wg.Add(1)

	// Tag the wait group since we are a new worker ready to go!
	wl.Log(workerId, "is ready for work!")

	var writer *implodeTfWriter
	for job := range q {
		// No writer and we got a job, lets make a new one.
		if writer == nil {
			fname := iFactory.GetNextTfRecordName(CLI.prefix)
			dstfp := strings.Join([]string{CLI.outputDir, fname}, string(filepath.Separator))
			writer = newimplodeTfWriter(workerId, dstfp)
		}

		wl.Log(workerId, "[%05d] -- got file path: %s", job.class, job.file)

		// Add it to the current writer.
		writer.AddFile(job.class, job.file)

		// If the count has satisfied the `num-shards`, the writer will be reset.
		if writer.count >= CLI.shardSize {
			writer.Flush()
			writer = nil
		}

		wl.Log(workerId, "is now idle")
	}

	// File queue is exhausted, we are done!
	wl.Log(workerId, "Worker is now done")
	wg.Done()
}

////////////////////////////////////////////////////////////////////////////////

func RunJob(nworkers int, args []string, wl *logger.WorkerLogger) error {
	// Setup args to parse.
	fs := flag.NewFlagSet("ImplodeTf", flag.ExitOnError)
	fs.StringVar(&CLI.inputDir, "input", "", "input directory to read images from")
	fs.StringVar(&CLI.outputDir, "output", "", "output directory to write images to")
	fs.StringVar(&CLI.prefix, "prefix", "", "output prefix for any files created")
	fs.StringVar(&CLI.filter, "filter", "", "specify classes to include, empty => include all")
	fs.IntVar(&CLI.shardSize, "shard-size", 1, "number of records per .tfrecord file")
	fs.IntVar(&CLI.maxCount, "max-count", 0, "maximum number of images per class, 0 => include all")
	fs.Parse(args)

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
	jobQ := make(chan *Job, CLI.numWorkers)

	// Make an implode mapper.
	im, err := newImplodeMapper(CLI.shardSize, CLI.maxCount, wl)
	if err != nil {
		return err
	}

	// Create workers, each one is responsible for decrementing his waitgroup
	// after it has exhausted the work queue.
	for wId := 0; wId < CLI.numWorkers; wId++ {
		go implodeTfWorker(wId, jobQ, &wg, wl)
	}

	// TODO: SHABA_BOILERPLATE_ITERATOR
	// The below code is SUPER boilerplate! and can be abstracted out fi the
	// signature for the worker queue is identical (which it should be ... )

	// Build the image implode mapper.
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
			im.AddFile(CLI.outputDir, CLI.inputDir, fp)

			c += 1
			wl.Status("found %d files so far ...", c)
		}
	} else {
		wl.Status("Building file list ... (this can take a while on large mounted dirs) ...")
		err := filepath.Walk(CLI.inputDir, func(fp string, fi os.FileInfo, err error) error {
			if err != nil {
				wl.Status("Warning found error walking dir. Error: %s", err.Error())
			} else {
				im.AddFile(CLI.outputDir, CLI.inputDir, fp)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	// Drain the imploder and feed work.
	for {
		job, err := im.Drain()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if jobs.RangeContainsClass(CLI.classes, job.class) {
			jobQ <- job
		}
	}

	wl.Status("Implode mapper done feeding jobs")

	// Done feeding work, file queue should be closed.
	close(jobQ)

	// Wait for all the records to finish getting written.
	wg.Wait()

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func init() {
	rand.Seed(time.Now().Unix())
}

////////////////////////////////////////////////////////////////////////////////
