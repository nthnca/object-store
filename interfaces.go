package objectstore

import (
	"context"
)

type storageClientInterface interface {
	list(ctx context.Context) storageListInterface
	readFile(ctx context.Context, filename string) ([]byte, error)
	writeFile(ctx context.Context, filename string, contents []byte) error
	deleteFile(ctx context.Context, filename string) error
}

type storageListInterface interface {
	next() (string, error)
}
