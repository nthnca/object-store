package objectstore

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
)

var testBucket = flag.String("bucket", "", "Bucket to use for integration tests")

func TestFullWorkCycle(t *testing.T) {
	if *testBucket == "" {
		t.Skip("Skipping integration test, this test uses a real GCS bucket.\n" +
			"If you would like to run these tests configure a GCS bucket and run\n" +
			"the tests with '-args -bucket=<BUCKET_NAME>")
		return
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	os1, err := New(ctx, client, *testBucket, "prefix/")
	if err != nil {
		log.Fatalf("Failed to create object store: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(z int) {
			defer wg.Done()
			for j := 0; j < 2; j++ {
				log.Printf("Saving: Object:s%d%d", z, j)
				os1.Insert(ctx, fmt.Sprintf("key%d%d", z, j),
					[]byte(fmt.Sprintf("data%d%d", z, j)))
				log.Printf("Saved: Object:s%d%d", z, j)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < 6; i++ {
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func(x, y int) {
				defer wg.Done()
				g := os1.Get(fmt.Sprintf("key%d%d", x, y))
				if bytes.Compare(g, []byte(fmt.Sprintf("data%d%d", x, y))) != 0 {
					log.Fatalf("oops")
				}
			}(i, j)
		}
	}
	wg.Wait()

	os1, err = New(ctx, client, *testBucket, "prefix/")
	if err != nil {
		log.Fatalf("Failed to create object store: %v", err)
	}

	for i := 0; i < 6; i++ {
		for j := 0; j < 2; j++ {
			wg.Add(1)
			go func(x, y int) {
				defer wg.Done()
				g := os1.Get(fmt.Sprintf("key%d%d", x, y))
				if bytes.Compare(g, []byte(fmt.Sprintf("data%d%d", x, y))) != 0 {
					log.Fatalf("oops")
				}
			}(i, j)
		}
	}
	wg.Wait()
}
