package memory_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/DataDog/temporal-large-payload-codec/internal/driver"
	"github.com/DataDog/temporal-large-payload-codec/internal/driver/memory"
	"github.com/DataDog/temporal-large-payload-codec/internal/server"
)

func TestDriver_PutPayload(t *testing.T) {
	var (
		ctx = context.Background()
		d   = memory.Driver{}
	)

	// Get missing payload
	_, err := d.GetPayload(ctx, &driver.GetRequest{Digest: "sha256:foobar"})
	if !errors.Is(err, server.ErrBlobNotFound) {
		t.Errorf("expected error %q, got %q", server.ErrBlobNotFound, err)
	}

	// Put a payload
	testPayloadBytes := []byte("hello world")
	if _, err := d.PutPayload(ctx, &driver.PutRequest{
		Payload:       bytes.NewReader(testPayloadBytes),
		Digest:        "sha256:test",
		ContentLength: uint64(len(testPayloadBytes)),
	}); err != nil {
		t.Fatal(err)
	}

	// Get the payload back out and compare to original bytes
	getResp, err := d.GetPayload(ctx, &driver.GetRequest{Digest: "sha256:test"})
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
