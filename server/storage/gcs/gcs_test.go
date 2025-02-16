// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package gcs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/gcs"
	"github.com/stretchr/testify/require"
)

func TestDriver(t *testing.T) {
	// To run this test locally comment on the t.Skip and set your bucket name
	t.Skip("Skipping this test since it only succeeds with Application Default Credentials setup and an actual backing bucket.")

	buf := bytes.Buffer{}
	ctx := context.Background()
	d, err := gcs.New(ctx, "<bucket-name>")
	require.NoError(t, err)

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

	// Delete the payload
	_, err = d.DeletePayload(ctx, &storage.DeleteRequest{Key: putResponse.Key})
	require.NoError(t, err)

	// Ensure the payload was deleted
	resp, err = d.ExistPayload(ctx, &storage.ExistRequest{Key: putResponse.Key})
	require.False(t, resp.Exists)
}
