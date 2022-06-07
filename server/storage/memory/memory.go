package memory

import (
	"bytes"
	"context"
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
	d.mux.Lock()
	defer d.mux.Unlock()

	b, err := io.ReadAll(request.Payload)
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
	d.mux.RLock()
	defer d.mux.RUnlock()

	if b, ok := d.blobs[request.Digest]; ok {
		return &storage.GetResponse{
			Data:          io.NopCloser(bytes.NewReader(b)),
			ContentLength: uint64(len(b)),
		}, nil
	}

	return nil, storage.ErrBlobNotFound
}
