package main

////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

func isBucketPath(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), "gs://")
}

////////////////////////////////////////////////////////////////////////////////

type bucketPath struct {
	bp      string
	bucket  string
	subpath string
}

func newBucketPath(s string) (*bucketPath, error) {
	if !isBucketPath(s) {
		return nil, errors.New("bucket paths should begin with gs://")
	}

	items := []string{}
	for _, item := range strings.Split(s[5:], string(filepath.Separator)) {
		if item != "" {
			items = append(items, item)
		}
	}

	if len(items[0]) == 0 {
		return nil, errors.New("bucket name not specified")
	}

	return &bucketPath{
		bp:      s,
		bucket:  items[0],
		subpath: strings.Join(items[1:], string(filepath.Separator)),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type bucketManager struct {
	bucket string
	ctxt   context.Context
	client *storage.Client
}

func newBucketManager(bucketName string) (*bucketManager, error) {
	ctxt := context.Background()
	client, err := storage.NewClient(ctxt)
	if err != nil {
		return nil, err
	}

	return &bucketManager{
		bucket: bucketName,
		ctxt:   ctxt,
		client: client,
	}, nil
}

func (bm *bucketManager) bucketUpload(dst *bucketPath, data []byte) error {
	wc := bm.client.Bucket(dst.bucket).Object(dst.subpath).NewWriter(bm.ctxt)
	defer wc.Close()

	_, err := wc.Write(data)
	return err
}

func (bm *bucketManager) bucketUploadFile(dst *bucketPath, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	wc := bm.client.Bucket(dst.bucket).Object(dst.subpath).NewWriter(bm.ctxt)
	if _, err := io.Copy(wc, f); err != nil {
		return err
	}
	return wc.Close()
}

func (bm *bucketManager) bucketDownload(src *bucketPath) ([]byte, error) {
	rc, err := bm.client.Bucket(src.bucket).Object(src.subpath).NewReader(bm.ctxt)
	if err != nil {
		return nil, err
	}

	bs := make([]byte, 0, rc.Size())
	if _, err := rc.Read(bs); err != nil {
		return nil, err
	}
	return bs, nil
}

func (bm *bucketManager) bucketDownloadFile(dst string, src *bucketPath) error {
	rc, err := bm.client.Bucket(src.bucket).Object(src.subpath).NewReader(bm.ctxt)
	if err != nil {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, rc); err != nil {
		return err
	}
	return rc.Close()
}

func (bm *bucketManager) getBucketIterator(prefix string) (*storage.ObjectIterator, error) {
	return bm.client.Bucket(bm.bucket).Objects(bm.ctxt, &storage.Query{Prefix: prefix}), nil
}

////////////////////////////////////////////////////////////////////////////////

var managers = map[string]*bucketManager{}
var managerMutex sync.RWMutex

func getBucketManager(bn string) *bucketManager {
	if bm, ok := managers[bn]; ok {
		return bm
	}

	managerMutex.Lock()
	defer managerMutex.Unlock()

	bm, err := newBucketManager(bn)
	fatalOnErr(err)

	managers[bn] = bm
	return bm
}

////////////////////////////////////////////////////////////////////////////////
