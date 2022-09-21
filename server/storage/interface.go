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

type Validatable interface {
	Validate(context.Context) error
}

type PutRequest struct {
	Metadata      map[string][]byte
	Data          io.Reader
	Key           string
	Digest        string
	ContentLength uint64
}

type PutResponse struct {
	// Key used to retrieve the stored data via a GetRequest.
	Key string
}

type GetRequest struct {
	Key    string
	Writer io.Writer
}

type GetResponse struct {
	ContentLength uint64
}
