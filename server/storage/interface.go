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
	Metadata      map[string][]byte
	Data          io.Reader
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
	ContentLength uint64
	Data          io.ReadCloser
}
