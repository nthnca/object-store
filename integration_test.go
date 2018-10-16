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

func GetData(i, j, gen int) []byte {
	return []byte(fmt.Sprintf("data-%d-key-%d-%d", gen, i, j))
}

func GetDataFromKey(key string, gen int) []byte {
	return []byte(fmt.Sprintf("data-%d-%s", gen, key))
}

func ValidateData(os *ObjectStore, i, j, gen int) {
	g := os.Get(GetKey(i, j))
	if bytes.Compare(g, GetData(i, j, gen)) != 0 {
		log.Fatalf("Validate fail, expected %s got %s", string(GetData(i, j, gen)),
			string(g))
	}
}

func PerformXY(funcxy func(x, y int)) {
	var wg sync.WaitGroup
	for x := 0; x < 6; x++ {
		wg.Add(1)
		go func(x_ int) {
			defer wg.Done()
			for y := 0; y < 2; y++ {
				funcxy(x_, y)
			}
		}(x)
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
	PerformXY(func(x, y int) {
		os1.Insert(ctx, GetKey(x, y), GetData(x, y, gen))
	})

	log.Printf("Validating Gen1")
	PerformXY(func(x, y int) {
		ValidateData(os1, x, y, gen)
	})

	log.Printf("Loading new ObjectStore")
	os1, err = New(ctx, client, *testBucket, "prefix/")
	if err != nil {
		log.Fatalf("Failed to create object store: %v", err)
	}

	log.Printf("Validating Gen1 (did persist and load work?)")
	PerformXY(func(x, y int) {
		ValidateData(os1, x, y, gen)
	})

	gen = 2

	log.Printf("Writing Gen2")
	PerformXY(func(x, y int) {
		os1.Insert(ctx, GetKey(x, y), GetData(x, y, gen))
	})

	log.Printf("Validating Gen2")
	PerformXY(func(x, y int) {
		ValidateData(os1, x, y, gen)
	})

	log.Printf("Validating Gen2 with ForEach")
	count := 0
	os1.ForEach(func(key string, value []byte) {
		count++
		d := GetDataFromKey(key, gen)
		if bytes.Compare(value, d) != 0 {
			log.Fatalf("Validate fail, expected %s got %v", string(d), string(value))
		}
	})
	if count != 12 {
		log.Fatalf("oops %d", count)
	}

	log.Printf("Deleting")
	PerformXY(func(x, y int) {
		os1.Delete(ctx, GetKey(x, y))
	})

	log.Printf("Validating deletions")
	PerformXY(func(x, y int) {
		d := os1.Get(GetKey(x, y))
		if d != nil {
			log.Fatalf("oops")
		}
	})

	log.Printf("Validating deletions with ForEach")
	os1.ForEach(func(key string, value []byte) {
		log.Fatalf("oops")
	})

	log.Printf("Loading new ObjectStore")
	os1, err = New(ctx, client, *testBucket, "prefix/")
	if err != nil {
		log.Fatalf("Failed to create object store: %v", err)
	}

	log.Printf("Validating Gen2 (did persist and load work?)")
	os1.ForEach(func(key string, value []byte) {
		log.Fatalf("oops")
	})

	gen = 3

	log.Printf("InsertBulk Gen3")
	objs := make(map[string][]byte)
	PerformXY(func(x, y int) {
		objs[GetKey(x, y)] = GetData(x, y, gen)
	})
	os1.InsertBulk(ctx, objs)

	log.Printf("Validating Gen3")
	PerformXY(func(x, y int) {
		ValidateData(os1, x, y, gen)
	})
}
