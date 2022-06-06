package minio

import (
	"context"

	"github.com/minio/minio-go/v7"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

type Driver struct {
	client minio.S3
}

func (d Driver) PutPayload(ctx context.Context, request *storage.PutRequest) (*storage.PutResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d Driver) GetPayload(ctx context.Context, request *storage.GetRequest) (*storage.GetResponse, error) {
	// TODO implement me
	panic("implement me")
}
