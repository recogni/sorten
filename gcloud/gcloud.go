////////////////////////////////////////////////////////////////////////////////

package gcloud

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

func IsBucketPath(s string) bool {
	return strings.HasPrefix(strings.ToLower(s), "gs://")
}

////////////////////////////////////////////////////////////////////////////////

type BucketPath struct {
	bp string

	Bucket  string
	Subpath string
}

func NewBucketPath(s string) (*BucketPath, error) {
	if !IsBucketPath(s) {
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

	return &BucketPath{
		bp: s,

		Bucket:  items[0],
		Subpath: strings.Join(items[1:], string(filepath.Separator)),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type BucketManager struct {
	Bucket string
	ctxt   context.Context
	client *storage.Client
}

func newBucketManager(bucketName string) (*BucketManager, error) {
	ctxt := context.Background()
	client, err := storage.NewClient(ctxt)
	if err != nil {
		return nil, err
	}

	return &BucketManager{
		ctxt:   ctxt,
		client: client,

		Bucket: bucketName,
	}, nil
}

func (bm *BucketManager) BucketUpload(dst *BucketPath, data []byte) error {
	wc := bm.client.Bucket(dst.Bucket).Object(dst.Subpath).NewWriter(bm.ctxt)
	defer wc.Close()

	_, err := wc.Write(data)
	return err
}

func (bm *BucketManager) BucketUploadFile(dst *BucketPath, src string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	wc := bm.client.Bucket(dst.Bucket).Object(dst.Subpath).NewWriter(bm.ctxt)
	if _, err := io.Copy(wc, f); err != nil {
		return err
	}
	return wc.Close()
}

func (bm *BucketManager) BucketDownload(src *BucketPath) ([]byte, error) {
	rc, err := bm.client.Bucket(src.Bucket).Object(src.Subpath).NewReader(bm.ctxt)
	if err != nil {
		return nil, err
	}

	bs := make([]byte, 0, rc.Size())
	if _, err := rc.Read(bs); err != nil {
		return nil, err
	}
	return bs, nil
}

func (bm *BucketManager) BucketDownloadFile(dst string, src *BucketPath) error {
	rc, err := bm.client.Bucket(src.Bucket).Object(src.Subpath).NewReader(bm.ctxt)
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

func (bm *BucketManager) GetBucketIterator(prefix string) (*storage.ObjectIterator, error) {
	return bm.client.Bucket(bm.Bucket).Objects(bm.ctxt, &storage.Query{Prefix: prefix}), nil
}

////////////////////////////////////////////////////////////////////////////////

var managers = map[string]*BucketManager{}
var managerMutex sync.RWMutex

func GetBucketManager(bn string) (*BucketManager, error) {
	if bm, ok := managers[bn]; ok {
		return bm, nil
	}

	managerMutex.Lock()
	defer managerMutex.Unlock()

	bm, err := newBucketManager(bn)
	if err == nil {
		managers[bn] = bm
	}
	return bm, err
}

////////////////////////////////////////////////////////////////////////////////
