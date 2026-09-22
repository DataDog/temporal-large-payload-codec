// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package gcs

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCopyPayloadPreservesMaxBytesError(t *testing.T) {
	body := http.MaxBytesReader(
		httptest.NewRecorder(),
		io.NopCloser(bytes.NewReader([]byte("too large"))),
		3,
	)

	err := copyPayload(io.Discard, body)
	require.Error(t, err)

	var maxBytesError *http.MaxBytesError
	require.ErrorAs(t, err, &maxBytesError)
}

func TestCopyPayloadPreservesOtherReadError(t *testing.T) {
	expected := errors.New("read failed")
	err := copyPayload(io.Discard, errorReader{err: expected})
	require.ErrorIs(t, err, expected)
}

type errorReader struct {
	err error
}

func (r errorReader) Read([]byte) (int, error) {
	return 0, r.err
}
