package storage

import (
	"context"
	"errors"
	"io"
)

var ErrBlobNotFound = errors.New("blob not found")

type Driver interface {
	PutPayload(context.Context, *PutRequest) (*PutResponse, error)
	GetPayload(context.Context, *GetRequest) (*GetResponse, error)
}

type PutRequest struct {
	Payload       io.Reader
	Digest        string
	ContentLength uint64
}

type PutResponse struct {
	// Optional URL where the blob was uploaded.
	Location string
}

type GetRequest struct {
	Digest string
}

type GetResponse struct {
	Data io.ReadCloser
}
