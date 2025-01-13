// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package memory_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestDriver(t *testing.T) {
	var (
		ctx = context.Background()
		d   = memory.Driver{}
		buf = bytes.Buffer{}
	)

	// Check missing payload
	resp, err := d.ExistPayload(ctx, &storage.ExistRequest{Key: "sha256:foobar"})
	require.NoError(t, err)
	require.False(t, resp.Exists)

	// Get missing payload
	_, err = d.GetPayload(ctx, &storage.GetRequest{Key: "sha256:foobar", Writer: &buf})
	var blobNotFound *storage.ErrBlobNotFound
	require.True(t, errors.As(err, &blobNotFound))
	require.Equal(t, buf.Len(), 0)

	// Put a payload
	testPayloadBytes := []byte("hello world")
	putResponse, err := d.PutPayload(ctx, &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Key:           "blobs/sha256:test",
		Digest:        "sha256:test",
		ContentLength: uint64(len(testPayloadBytes)),
	})
	require.NoError(t, err)
	require.NotEmpty(t, putResponse.Key)

	// Check payload exists
	resp, err = d.ExistPayload(ctx, &storage.ExistRequest{Key: putResponse.Key})
	require.NoError(t, err)
	require.True(t, resp.Exists)

	// Get the payload back out and compare to original bytes
	_, err = d.GetPayload(ctx, &storage.GetRequest{Key: putResponse.Key, Writer: &buf})
	require.NoError(t, err)

	// Read the data
	b, err := io.ReadAll(&buf)
	require.NoError(t, err)
	require.Equal(t, b, testPayloadBytes)

	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(testPayloadBytes) {
		t.Errorf("expected payload data %q, got %q", testPayloadBytes, b)
	}

	// Delete the payload
	_, err = d.DeletePayload(ctx, &storage.DeleteRequest{
		Key: "sha256:test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Ensure the payload was deleted
	resp, err = d.ExistPayload(ctx, &storage.ExistRequest{Key: "sha256:test"})
	require.False(t, resp.Exists)

}
