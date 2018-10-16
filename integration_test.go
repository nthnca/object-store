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

func GetKey(i, j int) string {
	return fmt.Sprintf("key-%d-%d", i, j)
}

func GetData(i, j, gen int) string {
	return []byte(fmt.Sprintf("data-%d-key-%d-%d", gen, i, j))
}

func GetDataFromKey(key string, gen int) string {
	return []byte(fmt.Sprintf("data-%d-%s", gen, key))
}

func PerformXY(max_x, max_y int, funcxy func(x, y int)) {
	var wg sync.WaitGroup
	for x := 0; x < max_x; x++ {
			for y := 0; y < max_y; y++ {
		wg.Add(1)
		go func(x_ int) {
			defer wg.Done()
				funcxy(x, y)
		}(x)
			}
	}
	wg.Wait()
}

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

	gen := 1
	log.Printf("Writing Gen1")
	PerformXY(6, 2, func(x, y int) {
				os1.Insert(ctx, GetKey(z, j), GetData(z, j, gen))
})

	log.Printf("Validating Gen1")
	PerformXY(6, 2, func(x, y int) {
 func(x, y int) {
				g := os1.Get(GetKey(x, y))
				if bytes.Compare(g, GetData(x, y, gen)) != 0 {
					log.Fatalf("oops")
				}
 })
		
	log.Printf("Loading new ObjectStore")
	os1, err = New(ctx, client, *testBucket, "prefix/")
	if err != nil {
		log.Fatalf("Failed to create object store: %v", err)
	}

		log.Printf("Validating Gen1 (did persist and load work?)")
	PerformXY(6, 2, func(x, y int) {
 func(x, y int) {
				g := os1.Get(GetKey(x, y))
				if bytes.Compare(g, GetData(x, y))) != 0 {
					log.Fatalf("oops")
				}

	log.Printf("Writing Gen2")
	gen = 2
		go func(z int) {
				os1.Insert(ctx, GetKey(z, j), GetData(z, j, gen))
			}

	 count := 0
	 os1.ForEach(func(key string, value []byte) {
		 count++;
		 d := GetDataFromKey(key)
				if bytes.Compare(value, d) != 0 {
					log.Fatalf("oops")
				}
	 })
}
