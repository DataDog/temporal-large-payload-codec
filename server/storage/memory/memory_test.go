package memory_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
)

func TestDriver_PutPayload(t *testing.T) {
	var (
		ctx = context.Background()
		d   = memory.Driver{}
	)

	// Get missing payload
	_, err := d.GetPayload(ctx, &storage.GetRequest{Digest: "sha256:foobar"})
	if !errors.Is(err, storage.ErrBlobNotFound) {
		t.Errorf("expected error %q, got %q", storage.ErrBlobNotFound, err)
	}

	// Put a payload
	testPayloadBytes := []byte("hello world")
	if _, err := d.PutPayload(ctx, &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Digest:        "sha256:test",
		ContentLength: uint64(len(testPayloadBytes)),
	}); err != nil {
		t.Fatal(err)
	}

	// Get the payload back out and compare to original bytes
	getResp, err := d.GetPayload(ctx, &storage.GetRequest{Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(getResp.Data)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(testPayloadBytes) {
		t.Errorf("expected payload data %q, got %q", testPayloadBytes, b)
	}
}
