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
	"log"
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

// KeyValueOperation allows you to interact with a given key, value pair.
type KeyValueOperation func(string, []byte)

// New will load the current state of an ObjectStore from the given bucket and
// prefix and return an ObjectStore handle for performing actions.
func New(ctx context.Context, client *storage.Client, bucketName, filePrefix string) (*ObjectStore, error) {
	var os ObjectStore
	os.client = client
	os.bucketName = bucketName
	os.filePrefix = filePrefix
	os.index = make(map[string]int)

	var wg sync.WaitGroup
	var load_err error
	ch_obj := make(chan *schema.ObjectSet)

	it := client.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: os.filePrefix})
	obj, iter_err := it.Next()
	for ; iter_err == nil && load_err == nil; obj, iter_err = it.Next() {
		// We don't want to prune the masterFileName.
		if obj.Name != os.filePrefix+masterFileName {
			os.files = append(os.files, obj.Name)
		}

		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			var tmp schema.ObjectSet
			err := os.load(ctx, filename, &tmp)
			if err != nil {
				load_err = err
			}
			ch_obj <- &tmp
		}(obj.Name)
	}

	go func() {
		wg.Wait()
		close(ch_obj)
	}()

	for objset := range ch_obj {
		os.addToData(objset)
	}

	if load_err != nil {
		return nil, fmt.Errorf("Failed to load: %v", load_err)
	}

	if iter_err != iterator.Done {
		return nil, fmt.Errorf("Failed to iterate through objects: %v",
			iter_err)
	}

	return &os, nil
}

// InsertBulk adds, updates, or deletes a group of key-values together.
func (os *ObjectStore) InsertBulk(ctx context.Context, index map[string][]byte) error {
	timestamp := time.Now().UnixNano()
	var objset schema.ObjectSet
	for key, object := range index {
		var obj schema.Object
		obj.Key = key
		obj.TimestampNanoSeconds = timestamp
		if len(object) == 0 {
			object = nil
		}
		obj.Object = object
		objset.Item = append(objset.Item, &obj)
	}

	filename, err := os.save(ctx, "", &objset)
	if err != nil {
		return fmt.Errorf("Failed to write: %v", err)
	}

	{
		os.mutex.Lock()
		defer os.mutex.Unlock()

		// Data successfully persisted, so safe to change our state
		os.addToData(&objset)
		os.prune(ctx, filename)
	}

	return nil
}

// Get returns the value associated with a given key.
func (os *ObjectStore) Get(key string) []byte {
	os.mutex.RLock()
	defer os.mutex.RUnlock()

	i, ok := os.index[key]
	if ok {
		return os.data.Item[i].Object
	}
	return nil
}

// All performs operation op on all key, value pairs in the ObjectStore. Note
// that all of these operations are run from inside a read lock so you
// will not be able to perform Insert/Delete operation.
func (os *ObjectStore) ForEach(op KeyValueOperation) {
	os.mutex.RLock()
	defer os.mutex.RUnlock()

	for _, obj := range os.data.Item {
		if obj.Object != nil {
			op(obj.Key, obj.Object)
		}
	}
}

// Insert adds or updates a value for a given key. If the key is already used
// this new value will replace the existing value.
func (os *ObjectStore) Insert(ctx context.Context, key string, object []byte) error {
	tmp := make(map[string][]byte)
	tmp[key] = object
	return os.InsertBulk(ctx, tmp)
}

// Delete removes a value for a given key.
func (os *ObjectStore) Delete(ctx context.Context, key string) error {
	tmp := make(map[string][]byte)
	tmp[key] = nil
	return os.InsertBulk(ctx, tmp)
}

// addToData merges objset with our master data and index.
func (os *ObjectStore) addToData(objset *schema.ObjectSet) {
	for _, obj := range objset.Item {
		i, ok := os.index[obj.Key]
		if ok {
			if obj.TimestampNanoSeconds > os.data.Item[i].TimestampNanoSeconds {
				os.data.Item[i] = obj
			}
		} else {
			os.index[obj.Key] = len(os.data.Item)
			os.data.Item = append(os.data.Item, obj)
		}
	}
}

func (os *ObjectStore) prune(ctx context.Context, filename string) {
	os.files = append(os.files, filename)
	if len(os.files) < 30 {
		return
	}

	files := os.files
	os.files = []string{}

	// Todo, if we marshalled the data here the save and deletes could
	// all happen post prune method return.
	_, err := os.save(ctx, masterFileName, &os.data)
	if err != nil {
		// We should log here I guess.
		return
	}

	var wg sync.WaitGroup
	for _, f := range files {
		wg.Add(1)
		go func(filename string) {
			defer wg.Done()
			err := os.client.Bucket(os.bucketName).Object(filename).Delete(ctx)
			if err != nil {
				// We don't care.
				log.Printf("Failed to delete %s: %v", filename, err)
			}
		}(f)
	}
	wg.Wait()
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

	filename = os.filePrefix + filename
	wc := os.client.Bucket(os.bucketName).Object(filename).NewWriter(ctx)
	checksum := md5.Sum(data)
	// Setting the checksum insures a partial file doesn't get uploaded.
	wc.MD5 = checksum[:]
	if _, err = wc.Write(data); err != nil {
		return "", fmt.Errorf("writing data: %v", err)
	}
	if err = wc.Close(); err != nil {
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

	if err = proto.Unmarshal(slurp, p); err != nil {
		return fmt.Errorf("unmarshalling proto: %v", err)
	}
	return nil
}
