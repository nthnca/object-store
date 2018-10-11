package objectstore

import (
	"context"
	"crypto/md5"
	"io/ioutil"

	"cloud.google.com/go/storage"
)

// storageClient provides operations on the set of data in an ObjectStore. Use
// New to get a handle.
type storageClient struct {
	// Client for accessing the storage.
	client *storage.Client

	// The bucket and prefix for this data.
	bucketName string
	filePrefix string
}

func (this *storageClient) list(ctx context.Context) storageListInterface {
	it := this.client.Bucket(this.bucketName).Objects(ctx, &storage.Query{Prefix: this.filePrefix})
	return &storageIter{iter: it, subsize: len(this.filePrefix)}
}

func (this *storageClient) readFile(ctx context.Context, filename string) ([]byte, error) {
	reader, err := this.client.Bucket(this.bucketName).Object(this.filePrefix + filename).NewReader(ctx)
	if err != nil {
		return nil, err
	}

	slurp, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		return nil, err
	}

	return slurp, nil
}

func (this *storageClient) writeFile(ctx context.Context, filename string, contents []byte) error {
	writer := this.client.Bucket(this.bucketName).Object(filename).NewWriter(ctx)
	checksum := md5.Sum(contents)

	// Setting the checksum insures a partial file doesn't get uploaded.
	writer.MD5 = checksum[:]
	if _, err := writer.Write(contents); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	return nil
}

func (this *storageClient) deleteFile(ctx context.Context, filename string) error {
	return this.client.Bucket(this.bucketName).Object(this.filePrefix + filename).Delete(ctx)
}

type storageIter struct {
	iter    *storage.ObjectIterator
	subsize int
}

func (this *storageIter) next() (string, error) {
	obj, err := this.iter.Next()
	if err != nil {
		return "", err
	}
	return obj.Name[this.subsize:], nil
}
