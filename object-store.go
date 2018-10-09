// Package mediainfo.
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
	"github.com/nthnca/curator/pkg/mediainfo/message"
	"github.com/nthnca/curator/pkg/util"
	"google.golang.org/api/iterator"
)

const (
	masterFileName = "master.md"
)

type MediaInfo struct {
  client *storage.Client
  
	bucketName string
  filePrefix string
  
  mutex RWMutex
	data       message.MediaSet
	index      map[[32]byte]int

  // List of temporary files that can be deleted after the next full write.
  tmpFiles      []string
}

// New will load the Media objects from the given bucket and return a MediaInfo object
// that will allow you to continue to add Media objects to this bucket.
func New(ctx context.Context, client *storage.Client, bucketName string) (*MediaInfo, error) {
	var mi MediaInfo
  os.client = client
	mi.bucketName = bucketName
	mi.index = make(map[[32]byte]int)

	t := time.Now().UnixNano()
	log.Printf("Reading MediaInfo: %s", bucketName)
	if err := load(ctx, client, bucketName, masterFileName, &mi.data); err != nil {
		if err != storage.ErrObjectNotExist {
			return nil, fmt.Errorf("loading masterfile: (%s) %v", bucketName, err)
		}
	}

	for i, m := range mi.data.Media {
		mi.index[util.Sha256(m.Key)] = i
	}

	ch := make(chan *message.Media)
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

		mi.files = append(mi.files, obj.Name)
		wg.Add(1)
		go func() {
			defer wg.Done()
			var tmp message.Media
			err := load(ctx, client, bucketName, obj.Name, &tmp)
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
		mi.insertInternal(ctx, client, m, false)
	}
	mi.saveAll(ctx, client)
	log.Printf("Read %d Media objects, took %v seconds",
		len(mi.All()), float64(time.Now().UnixNano()-t)/1000000000.0)
	return &mi, nil
}

// Insert saves a new Media object. If this objects Key is the same as an existing object it will
// replace it if its timestamp is newer, if this new object is older it will drop it.
func (mi *MediaInfo) Insert(ctx context.Context, string key, object []byte) {
  os.mutex.Lock()
  defer os.mutex.Unlock()
	i, ok := mi.index[key]
	if ok {
		mi.data.Media[i] = media
	} else {
		mi.index[key] = len(mi.data.Media)
		mi.data.Media = append(mi.data.Media, media)
	}

  if err := mi.saveOne(ctx, client, media); err != nil {
		log.Fatalf("Failed to write: %v", err)
	}
}

// DeleteFast deletes a referenced Media object but doesn't save. To save you need to call Flush.
func (mi *MediaInfo) Delete(key [32]byte) {
  os.mutex.Lock()
  defer os.mutex.Unlock()
	i, ok := mi.index[key]
	if !ok {
		return
	}

	delete(mi.index, key)
	mi.data.Media[i] = mi.data.Media[len(mi.data.Media)-1]
	mi.index[util.Sha256(mi.data.Media[i].Key)] = i
	mi.data.Media = mi.data.Media[:len(mi.data.Media)-1]
}

func (mi *MediaInfo) Get(key [32]byte) *message.Media {
  os.mutex.RLock()
  defer os.mutex.RUnlock()
	var rv message.Media
	i, ok := mi.index[key]
	if !ok {
		return nil
	}
	proto.Merge(&rv, mi.data.Media[i])
	return &rv
}

func (mi *MediaInfo) All() []*message.Media {
	return mi.data.Media
}

func load(ctx context.Context, client *storage.Client, bucketName, filename string, p proto.Message) error {
	reader, err := client.Bucket(bucketName).Object(filename).NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return err
		}
		return fmt.Errorf("creating new reader: %v", err)
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

func (mi *MediaInfo) save(ctx context.Context, client *storage.Client, bucketName, filename string, p proto.Message) error {
	data, err := proto.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshalling proto: %v", err)
	}

	wc := client.Bucket(bucketName).Object(filename).NewWriter(ctx)
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

func (mi *MediaInfo) saveAll(ctx context.Context, client *storage.Client) error {
	if err := mi.save(ctx, client, mi.bucketName, masterFileName, &mi.data); err != nil {
		return fmt.Errorf("saving proto: %v", err)
	}

	for i, _ := range mi.files {
		mi.wait.Add(1)
		filename := mi.files[i]
		go func() {
			err := client.Bucket(mi.bucketName).Object(filename).Delete(ctx)
			if err != nil {
				// This seems to fail sometimes, but still deletes...
				// At any rate we don't really care.
				// log.Printf("Failed to delete: %v", err)
			}
			mi.wait.Done()
		}()
	}
	mi.files = []string{}

	return nil
}

func (mi *MediaInfo) saveOne(ctx context.Context, client *storage.Client, m *message.Media) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshalling proto: %v", err)
	}

	shasum := sha256.Sum256(data)
	filename := fmt.Sprintf("%s.md", hex.EncodeToString(shasum[:]))

	if err := mi.save(ctx, client, mi.bucketName, filename, m); err != nil {
		return fmt.Errorf("saving proto: %v", err)
	}

	mi.files = append(mi.files, filename)
	return nil
}
