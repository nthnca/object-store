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

	// List of temporary files that can be deleted after the next full
	// write.
	files []string
}

// New will load the current state of an ObjectStore from the given bucket and
// prefix and return an ObjectStore handle for performing actions.
func New(ctx context.Context, client *storage.Client, bucketName string) (*ObjectStore, error) {
	var os ObjectStore
	os.client = client
	os.bucketName = bucketName
	os.index = make(map[string]int)

	t := time.Now().UnixNano()
	log.Printf("Reading ObjectStore: %s", bucketName)

	var wg sync.WaitGroup
	ch := make(chan *schema.ObjectSet)

	bucket := client.Bucket(bucketName)
	for it := bucket.Objects(ctx, nil); ; {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to iterate through objects: %v", err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			var tmp schema.ObjectSet
			err := os.load(ctx, obj.Name, &tmp)
			if err != nil {
				log.Fatalf("Failed to load: %v (%v)", err, obj.Name)
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
			// Fix, we don't need the lock in this case.
			os.addToData(o)
		}
	}

	// os.saveAll(ctx, client)

	log.Printf("Read %d Media objects, took %v seconds",
		len(os.data.Item), float64(time.Now().UnixNano()-t)/1000000000.0)
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
	if err := os.save(ctx, "", &tmp); err != nil {
		log.Fatalf("Failed to write: %v", err)
	}

	return nil
}

// Delete removes a value for a given key.
func (os *ObjectStore) Delete(ctx context.Context, key string) error {
	os.Insert(ctx, key, nil)
	return nil
}

// Get returns the value associated with a given key.
func (os *ObjectStore) Get(key string) ([]byte, error) {
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
		os.index[obj.Key] = len(os.data.Item)
		os.data.Item = append(os.data.Item, obj)
	}
}

/*
// DeleteFast deletes a referenced Media object but doesn't save. To save you need to call Flush.

func (os *ObjectStore) Get(key [32]byte) *[]byte {
	os.mutex.RLock()
	defer os.mutex.RUnlock()
	i, ok := os.index[key]
	if !ok {
		return nil
	}

	return i
}

// How to deal with this when we use locks? ...
func (os *ObjectStore) All() []*object {
	return os.data.Media
}
*/

func (os *ObjectStore) save(ctx context.Context, filename string, p *schema.ObjectSet) error {
	data, err := proto.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshalling proto: %v", err)
	}

	if filename == "" {
		shasum := sha256.Sum256(data)
		filename = fmt.Sprintf("%s.os", hex.EncodeToString(shasum[:]))
	}

	wc := os.client.Bucket(os.bucketName).Object(filename).NewWriter(ctx)
	checksum := md5.Sum(data)
	wc.MD5 = checksum[:]
	if _, err := wc.Write(data); err != nil {
		return fmt.Errorf("writing data: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("closing file: %v", err)
	}
	return nil
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
