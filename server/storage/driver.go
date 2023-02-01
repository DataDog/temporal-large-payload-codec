// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

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
	ExistPayload(context.Context, *ExistRequest) (*ExistResponse, error)
	DeletePayload(context.Context, *DeleteRequest) (*DeleteResponse, error)
}

type Validatable interface {
	Validate(context.Context) error
}

type PutRequest struct {
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

type ExistRequest struct {
	Key string
}

type ExistResponse struct {
	Exists bool
}

type DeleteRequest struct {
	Key string
}

type DeleteResponse struct {
}
