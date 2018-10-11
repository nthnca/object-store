// Package objectstore provides persistent key-value storage, using
// Google cloud storage to persist the data.
package objectstore

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/golang/protobuf/proto"
	"github.com/nthnca/object-store/internal/schema"
	"google.golang.org/api/iterator"
)

const (
	masterFileName = "master.md"
)

// ObjectStore provides operations on the set of data in an ObjectStore. Use
// New to get a handle.
type ObjectStore struct {
	// Client for accessing the storage.
	client *storage.Client

	// The bucket and prefix for this data.
	bucketName string
	filePrefix string

	// RWMutex to protect data and index.
	mutex sync.RWMutex
	data  schema.ObjectSet
	index map[string]int

	// Files that can be deleted after the next full write.
	files []string
}

// New will load the current state of an ObjectStore from the given bucket and
// prefix and return an ObjectStore handle for performing actions.
func New(ctx context.Context, client *storage.Client, bucketName, filePrefix string) (*ObjectStore, error) {
	var os ObjectStore
	os.client = client
	os.bucketName = bucketName
	os.filePrefix = filePrefix
	os.index = make(map[string]int)

	var wg sync.WaitGroup
	var oops error
	oops = nil
	ch := make(chan *schema.ObjectSet)

	bucket := client.Bucket(bucketName)
	for it := bucket.Objects(ctx, nil); ; {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			return nil, fmt.Errorf("Failed to iterate through objects: %v", err)
		}

		// We don't want to prune the masterFileName.
		if obj.Name != masterFileName {
			os.files = append(os.files, obj.Name)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var tmp schema.ObjectSet
			err := os.load(ctx, obj.Name, &tmp)
			if err != nil {
				oops = fmt.Errorf("Failed to load: %v (%v)", err, obj.Name)
			}
			ch <- &tmp
		}()
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for obj := range ch {
		for _, o := range obj.Item {
			os.addToData(o)
		}
	}

	if oops != nil {
		return nil, oops
	}

	return &os, nil
}

// Insert adds or updates a value for a given key. If the key is already used
// this new value will replace the existing value.
func (os *ObjectStore) Insert(ctx context.Context, key string, object []byte) error {
	var obj schema.Object
	obj.Key = key
	obj.TimestampNanoSeconds = time.Now().UnixNano()
	obj.Object = object

	os.addToData(&obj)

	var tmp schema.ObjectSet
	tmp.Item = append(tmp.Item, &obj)
	filename, err := os.save(ctx, "", &tmp)
	if err != nil {
		return fmt.Errorf("Failed to write: %v", err)
	}

	os.prune(ctx, filename)

	return nil
}

// Delete removes a value for a given key.
func (os *ObjectStore) Delete(ctx context.Context, key string) error {
	os.Insert(ctx, key, nil)
	return nil
}

// Get returns the value associated with a given key.
func (os *ObjectStore) Get(key string) ([]byte, error) {
	os.mutex.Lock()
	defer os.mutex.Unlock()

	i, ok := os.index[key]
	if ok {
		return os.data.Item[i].Object, nil
	}
	return nil, nil
}

// KeyValueOperation allows you to interact with a given key, value pair.
type KeyValueOperation func(string, []byte)

// All performs operation op on all key, value pairs in the ObjectStore. Note
// that all of these operations are run from inside a read lock so you
// will not be able to perform Insert/Delete operation.
func (os *ObjectStore) All(op KeyValueOperation) {
}

// addToData takes the write lock and adds this schema.Object to the data and
// index.
func (os *ObjectStore) addToData(obj *schema.Object) {
	os.mutex.Lock()
	defer os.mutex.Unlock()

	i, ok := os.index[obj.Key]
	if ok {
		if obj.TimestampNanoSeconds > os.data.Item[i].TimestampNanoSeconds {
			os.data.Item[i] = obj
		}
	} else {
		if len(obj.Object) > 0 {
			os.index[obj.Key] = len(os.data.Item)
			os.data.Item = append(os.data.Item, obj)
		}
	}
}

func (os *ObjectStore) prune(ctx context.Context, filename string) {
	os.mutex.Lock()
	defer os.mutex.Unlock()

	os.files = append(os.files, filename)
	if len(os.files) < 30 {
		return
	}

	// TODO make sure this saved!!!
	_, err := os.save(ctx, masterFileName, &os.data)
	if err != nil {
		// We should log here I guess.
		return
	}

	var wg sync.WaitGroup
	for _, f := range os.files {
		x := f
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := os.client.Bucket(os.bucketName).Object(x).Delete(ctx)
			if err != nil {
				// We don't care that much, but should still log.
			}
		}()
	}
	wg.Wait()
	os.files = []string{}
}

func (os *ObjectStore) save(ctx context.Context, filename string, p *schema.ObjectSet) (string, error) {
	data, err := proto.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("marshalling proto: %v", err)
	}

	if filename == "" {
		shasum := sha256.Sum256(data)
		filename = fmt.Sprintf("%s.os", hex.EncodeToString(shasum[:]))
	}

	wc := os.client.Bucket(os.bucketName).Object(filename).NewWriter(ctx)
	checksum := md5.Sum(data)
	wc.MD5 = checksum[:]
	if _, err := wc.Write(data); err != nil {
		return "", fmt.Errorf("writing data: %v", err)
	}
	if err := wc.Close(); err != nil {
		return "", fmt.Errorf("closing file: %v", err)
	}
	return filename, nil
}

func (os *ObjectStore) load(ctx context.Context, filename string, p *schema.ObjectSet) error {
	reader, err := os.client.Bucket(os.bucketName).Object(filename).NewReader(ctx)
	if err != nil {
		// Why do we handle this error specially?
		if err == storage.ErrObjectNotExist {
			return err
		}
		return fmt.Errorf("Opening file: %v", err)
	}

	slurp, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		return fmt.Errorf("trying to read: %v", err)
	}

	if err := proto.Unmarshal(slurp, p); err != nil {
		return fmt.Errorf("unmarshalling proto: %v", err)
	}
	return nil
}
