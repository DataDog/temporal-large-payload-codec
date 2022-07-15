package memory

import (
	"bytes"
	"context"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"io"
	"sync"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

var _ storage.Driver = &Driver{}

type Driver struct {
	mux sync.RWMutex
	// Map of blob digests (in the form `sha256:deadbeef`) to data
	blobs map[string][]byte
}

func (d *Driver) PutPayload(ctx context.Context, request *storage.PutRequest) (*storage.PutResponse, error) {
	var err error

	span, _ := tracer.SpanFromContext(ctx)
	span.SetOperationName("PutPayload - Memory")
	defer span.Finish(tracer.WithError(err))

	d.mux.Lock()
	defer d.mux.Unlock()

	b, err := io.ReadAll(request.Data)
	if err != nil {
		return nil, err
	}

	if d.blobs == nil {
		d.blobs = make(map[string][]byte)
	}
	d.blobs[request.Digest] = b

	return &storage.PutResponse{
		Location: request.Digest,
	}, nil
}

func (d *Driver) GetPayload(ctx context.Context, request *storage.GetRequest) (*storage.GetResponse, error) {
	var err error

	span, _ := tracer.SpanFromContext(ctx)
	span.SetOperationName("GetPayload - Memory")
	defer span.Finish(tracer.WithError(err))

	d.mux.RLock()
	defer d.mux.RUnlock()

	if b, ok := d.blobs[request.Digest]; ok {
		if _, err := io.Copy(request.Writer, bytes.NewReader(b)); err != nil {
			return nil, err
		}

		return &storage.GetResponse{}, nil
	}

	return nil, storage.ErrBlobNotFound
}
