package storage

import (
	"context"
	"fmt"
	"io"
)

type ErrBlobNotFound struct {
	Err error
}

func (m *ErrBlobNotFound) Error() string {
	return fmt.Sprintf("blob not found: %v", m.Err)
}

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
	Writer io.Writer
}

type GetResponse struct {
	ContentLength uint64
}
