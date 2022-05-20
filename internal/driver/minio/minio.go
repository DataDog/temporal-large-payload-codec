package minio

import (
	"context"

	"github.com/minio/minio-go/v7"

	"github.com/DataDog/temporal-large-payload-codec/internal/driver"
)

type Driver struct {
	client minio.S3
}

func (d Driver) PutPayload(ctx context.Context, request *driver.PutRequest) (*driver.PutResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d Driver) GetPayload(ctx context.Context, request *driver.GetRequest) (*driver.GetResponse, error) {
	// TODO implement me
	panic("implement me")
}
