package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestGetBlobV1(t *testing.T) {
	driver := &memory.Driver{}
	testPayloadBytes := []byte("hello world")
	putResponse, err := driver.PutPayload(context.Background(), &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Digest:        "sha256:test",
		ContentLength: uint64(len(testPayloadBytes)),
	})

	require.NoError(t, err)

	testCase := []struct {
		name        string
		target      string
		method      string
		headers     map[string]string
		queryParams map[string]string
		want        string
		statusCode  int
	}{
		{
			name:   "No Content type specified",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `missing or incorrect Content-Type header`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Wrong Content type specified",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "text/*",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `missing or incorrect Content-Type header`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Missing digest",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{},
			want:        `digest query parameter is required`,
			statusCode:  http.StatusBadRequest,
		},
		{
			name:   "Missing length",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type": "application/octet-stream",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `expected content length header is required`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "No content",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `blob not found: <nil>`,
			statusCode: http.StatusNotFound,
		},
		{
			name:   "Successful retrieval",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": putResponse.Location,
			},
			want:       `hello world`,
			statusCode: http.StatusOK,
		},
	}

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			request := httptest.NewRequest(scenario.method, fmt.Sprintf("/v1/%s", scenario.target), nil)
			// add headers
			for k, v := range scenario.headers {
				request.Header.Set(k, v)
			}

			// add query parameters
			q := request.URL.Query()
			for k, v := range scenario.queryParams {
				q.Add(k, v)
			}
			request.URL.RawQuery = q.Encode()

			responseRecorder := httptest.NewRecorder()

			handler := NewHttpHandler(driver)
			handler.ServeHTTP(responseRecorder, request)

			require.Equal(t, scenario.statusCode, responseRecorder.Code)
			require.Equal(t, scenario.want, responseRecorder.Body.String())
		})
	}
}

func TestGetBlobV2(t *testing.T) {
	driver := &memory.Driver{}
	testPayloadBytes := []byte("hello world")
	putResponse, err := driver.PutPayload(context.Background(), &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Digest:        "sha256:3b336ba10c19d14d5e741d7b76957bb88620a282d92aac23e2d81c2393f1451d",
		ContentLength: uint64(len(testPayloadBytes)),
	})
	require.NoError(t, err)

	testCase := []struct {
		name        string
		target      string
		method      string
		headers     map[string]string
		queryParams map[string]string
		want        string
		statusCode  int
	}{
		{
			name:   "No Content type specified",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `missing or incorrect Content-Type header`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Wrong Content type specified",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "text/*",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `missing or incorrect Content-Type header`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Missing digest",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{},
			want:        `digest query parameter is required`,
			statusCode:  http.StatusBadRequest,
		},
		{
			name:   "Wrong digest format",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "12345",
			},
			want:       `invalid digest format '12345'`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Missing length",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type": "application/octet-stream",
			},
			queryParams: map[string]string{
				"digest": "sha256:12345",
			},
			want:       `expected content length header is required`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Wrong hash function",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "sha512:12345",
			},
			want:       `invalid hash type 'sha512'`,
			statusCode: http.StatusBadRequest,
		},
		{
			name:   "Wrong digest",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": "sha256:12345",
			},
			want:       `blob not found: <nil>`,
			statusCode: http.StatusNotFound,
		},
		{
			name:   "Successful retrieval",
			target: "blobs/get",
			method: http.MethodGet,
			headers: map[string]string{
				"Content-Type":                      "application/octet-stream",
				"X-Payload-Expected-Content-Length": "10",
			},
			queryParams: map[string]string{
				"digest": putResponse.Location,
			},
			want:       `hello world`,
			statusCode: http.StatusOK,
		},
	}

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			request := httptest.NewRequest(scenario.method, fmt.Sprintf("/v2/%s", scenario.target), nil)
			// add headers
			for k, v := range scenario.headers {
				request.Header.Set(k, v)
			}

			// add query parameters
			q := request.URL.Query()
			for k, v := range scenario.queryParams {
				q.Add(k, v)
			}
			request.URL.RawQuery = q.Encode()

			responseRecorder := httptest.NewRecorder()

			handler := NewHttpHandler(driver)
			handler.ServeHTTP(responseRecorder, request)

			assert.Equal(t, scenario.statusCode, responseRecorder.Code)
			assert.Equal(t, scenario.want, responseRecorder.Body.String())
		})
	}
}
