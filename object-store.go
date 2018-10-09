// Package objectStore.
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
	"github.com/nthnca/object-store/schema"
	"google.golang.org/api/iterator"
)

const (
	masterFileName = "master.md"
)

type object struct {
	wg   sync.WaitGroup
	key  string
	data []byte
}

type objectStore struct {
	client *storage.Client

	bucketName string
	filePrefix string

	mutex sync.RWMutex
	data  schema.ObjectSet
	index map[string]int

	// List of temporary files that can be deleted after the next full write.
	files []string
}

// New will load the Media objects from the given bucket and return a objectStore object
// that will allow you to continue to add Media objects to this bucket.
func New(ctx context.Context, client *storage.Client, bucketName string) (*objectStore, error) {
	var os objectStore
	os.client = client
	os.bucketName = bucketName
	os.index = make(map[string]int)

	t := time.Now().UnixNano()
	log.Printf("Reading objectStore: %s", bucketName)
	if err := os.load(ctx, masterFileName, os.data); err != nil {
		if err != storage.ErrObjectNotExist {
			return nil, fmt.Errorf("loading masterfile: (%s) %v", bucketName, err)
		}
	}

	for i, m := range os.data.Item {
		os.index[m.Key] = i
	}

	ch := make(chan *object)
	var wg sync.WaitGroup

	bucket := client.Bucket(bucketName)
	for it := bucket.Objects(ctx, nil); ; {
		obj, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("Failed to iterate through objects: %v", err)
		}
		if obj.Name == masterFileName {
			continue
		}

		os.files = append(os.files, obj.Name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var tmp schema.ObjectSet
			err := os.load(ctx, obj.Name, tmp)
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

	for m := range ch {
		os.insertInternal(ctx, client, m, false)
	}
	os.saveAll(ctx, client)
	log.Printf("Read %d Media objects, took %v seconds",
		len(os.All()), float64(time.Now().UnixNano()-t)/1000000000.0)
	return &os, nil
}

// Insert saves a new Media object. If this objects Key is the same as an existing object it will
// replace it if its timestamp is newer, if this new object is older it will drop it.
func (os *objectStore) Insert(ctx context.Context, key string, object []byte) {
	var obj schema.Object
	obj.Key = key
	obj.Object = object

	{
		os.mutex.Lock()
		defer os.mutex.Unlock()

		// We wait until we have the lock to set the timestamp.
		obj.TimestampNanoSeconds = time.Now().UnixNano()

		i, ok := os.index[key]
		if ok {
			os.data.Item[i] = &obj
		} else {
			os.index[key] = len(os.data.Item)
			os.data.Item = append(os.data.Item, &obj)
		}
	}

	var tmp schema.ObjectSet
	tmp.Item = append(tmp.Item, &obj)
	if err := os.save(ctx, "", &tmp); err != nil {
		log.Fatalf("Failed to write: %v", err)
	}
}

/*
// DeleteFast deletes a referenced Media object but doesn't save. To save you need to call Flush.
func (os *objectStore) Delete(key [32]byte) {
	Insert(ctx, key, nil)
}

func (os *objectStore) Get(key [32]byte) *[]byte {
	os.mutex.RLock()
	defer os.mutex.RUnlock()
	i, ok := os.index[key]
	if !ok {
		return nil
	}

	return i
}

// How to deal with this when we use locks? ...
func (os *objectStore) All() []*object {
	return os.data.Media
}
*/

func (os *objectStore) save(ctx context.Context, filename string, p *schema.ObjectSet) error {
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

func (os *objectStore) load(ctx context.Context, filename string, p *schema.ObjectSet) error {
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
