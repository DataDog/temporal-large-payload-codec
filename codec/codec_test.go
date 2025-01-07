// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package codec

import (
	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"go.temporal.io/api/common/v1"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	updateEncodedPayload = false
)

func TestV2Codec(t *testing.T) {
	testCase := []struct {
		name           string
		payload        common.Payload
		encodedPayload common.Payload
	}{
		{
			name: "no large payload encoding needed",
			payload: common.Payload{
				Metadata: map[string][]byte{
					"foo": []byte("bar"),
				},
				Data: []byte("hello world"),
			},
			encodedPayload: common.Payload{
				Metadata: map[string][]byte{
					"foo": []byte("bar"),
				},
			},
		},
		{
			name: "large payload with prefix",
			payload: common.Payload{
				Metadata: map[string][]byte{
					"foo":                     []byte("bar"),
					"baz":                     []byte("qux"),
					"remote-codec/key-prefix": []byte("1234"),
				},
				Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
			},
			encodedPayload: common.Payload{
				Metadata: map[string][]byte{
					"encoding":                 []byte("json/plain"),
					"temporal.io/remote-codec": []byte("v2"),
				},
			},
		},
		{
			name: "large payload no prefix",
			payload: common.Payload{
				Metadata: map[string][]byte{
					"foo": []byte("bar"),
					"baz": []byte("qux"),
				},
				Data: []byte("This message is also longer than the 32 bytes limit!"),
			},
			encodedPayload: common.Payload{
				Metadata: map[string][]byte{
					"encoding":                 []byte("json/plain"),
					"temporal.io/remote-codec": []byte("v2"),
				},
			},
		},
	}

	s, c, _ := setUp(t, "v2")
	defer s.Close()

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			actualEncodedPayload, err := c.Encode([]*common.Payload{&scenario.payload})
			require.NoError(t, err)

			if updateEncodedPayload {
				toFile(t, actualEncodedPayload[0].Data)
			}

			// load the encoded payload from file
			scenario.encodedPayload.Data = fromFile(t)

			require.Equal(t, &scenario.encodedPayload, actualEncodedPayload[0])

			actualPayload, err := c.Decode([]*common.Payload{&scenario.encodedPayload})
			require.NoError(t, err)

			require.Equal(t, &scenario.payload, actualPayload[0])
		})
	}
}

func Test_setting_withDecodeOnly_disables_encoding(t *testing.T) {
	d := &memory.Driver{}
	srv := httptest.NewServer(server.NewHttpHandler(d))
	defer srv.Close()
	decodeOnlyCodec := setUpWithServer(t, "v2", srv, true)

	defaultCodec := setUpWithServer(t, "v2", srv, false)

	payload := common.Payload{
		Metadata: map[string][]byte{
			"foo":                     []byte("bar"),
			"baz":                     []byte("qux"),
			"remote-codec/key-prefix": []byte("1234"),
		},
		Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
	}

	resp1, err := decodeOnlyCodec.Encode([]*common.Payload{&payload})
	require.NoError(t, err)

	require.Equal(t, &payload, resp1[0])

	resp2, err := defaultCodec.Encode([]*common.Payload{&payload})
	require.NoError(t, err)
	require.NotEqual(t, &resp1[0], resp2[0])
	require.NotEqual(t, &payload, resp2[0])

	decodedResp2, err := decodeOnlyCodec.Decode(resp2)
	require.NoError(t, err)

	require.Equal(t, &payload, decodedResp2[0])
}

func Test_the_same_payload_can_be_encoded_multiple_times(t *testing.T) {
	s, c, _ := setUp(t, "v2")
	defer s.Close()

	payload := common.Payload{
		Metadata: map[string][]byte{
			"foo":                     []byte("bar"),
			"baz":                     []byte("qux"),
			"remote-codec/key-prefix": []byte("1234"),
		},
		Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
	}

	resp1, err := c.Encode([]*common.Payload{&payload})
	if err != nil {
		require.NoError(t, err)
	}

	resp2, err := c.Encode([]*common.Payload{&payload})
	if err != nil {
		require.NoError(t, err)
	}

	require.Equal(t, resp1, resp2)
}

func Test_codec_sets_custom_headers_when_sending_request_to_lps(t *testing.T) {
	expectedSingleHeaderKey := "SINGLE_HEADER"
	expectedMultiHeaderKey := "MULTI_HEADER"
	expectedHeaders := map[string][]string{
		expectedSingleHeaderKey: {"VALUE"},
		expectedMultiHeaderKey:  {"VALUE1", "VALUE2"},
	}

	testSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for header, values := range expectedHeaders {
			gotValues := r.Header.Values(header)
			require.Equal(t, values, gotValues)
		}

		w.WriteHeader(200)
	}))
	defer testSrv.Close()

	client, err := New(
		WithURL(testSrv.URL),
		WithHTTPClient(testSrv.Client()),
		WithNamespace("test"),
		WithMinBytes(32),
		WithCustomHeader(expectedSingleHeaderKey, expectedHeaders[expectedSingleHeaderKey][0]),
		WithCustomHeader(expectedMultiHeaderKey, expectedHeaders[expectedMultiHeaderKey][0]),
		WithCustomHeader(expectedMultiHeaderKey, expectedHeaders[expectedMultiHeaderKey][1]),
	)

	require.NoError(t, err)
	require.NotNil(t, client)

	payload := common.Payload{
		Metadata: map[string][]byte{
			"foo":                     []byte("bar"),
			"baz":                     []byte("qux"),
			"remote-codec/key-prefix": []byte("1234"),
		},
		Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
	}

	_, _ = client.Encode([]*common.Payload{&payload})
}

func TestNewCodec(t *testing.T) {
	d := &memory.Driver{}
	s := httptest.NewServer(server.NewHttpHandler(d))

	// valid client
	client, err := New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithMinBytes(32),
	)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, "v2", client.version)

	// missing URL
	client, err = New(
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithMinBytes(32),
	)
	require.Error(t, err)
	require.Nil(t, client)

	// no namespace
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithMinBytes(32),
	)
	require.Error(t, err)
	require.Nil(t, client)

	// HTTP client is optional
	client, err = New(
		WithURL(s.URL),
		WithNamespace("test"),
		WithMinBytes(32),
	)
	require.NoError(t, err)
	require.NotNil(t, client)

	// min bytes optional
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
	)
	require.NoError(t, err)
	require.NotNil(t, client)
	require.Equal(t, 128000, client.minBytes) // 128KB

	// v1
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithVersion("v1"),
	)
	require.Error(t, err)

	// invalid version
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithVersion("v3"),
	)
	require.Error(t, err)

	// without URL health check during init.
	client, err = New(
		WithURL("INVALID URL"),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithVersion("v2"),
		WithoutUrlHealthCheck(),
	)
	require.NoError(t, err)

	// with empty header key
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithVersion("v2"),
		WithoutUrlHealthCheck(),
		WithCustomHeader("", "VALID_VALUE"),
	)
	require.Error(t, err)

	// with invalid header key
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithVersion("v2"),
		WithoutUrlHealthCheck(),
		WithCustomHeader("invalid header", "VALID_VALUE"),
	)
	require.Error(t, err)

	// with valid customHeader
	client, err = New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithVersion("v2"),
		WithoutUrlHealthCheck(),
		WithCustomHeader("VALID0SINGLE", "VALID_VALUE"),
		WithCustomHeader("VALID-MULTI", "VALUE_1"),
		WithCustomHeader("VALID-MULTI", "VALUE_2"),
	)
	require.NoError(t, err)

}

func setUp(t *testing.T, version string) (*httptest.Server, *Codec, storage.Driver) {
	// Create test remote codec service
	d := &memory.Driver{}
	s := httptest.NewServer(server.NewHttpHandler(d))

	// Create test codec (to be used from Go SDK)
	c := setUpWithServer(t, version, s, false)

	return s, c, d
}

func setUpWithServer(t *testing.T, version string, server *httptest.Server, withDecodeOnly bool) *Codec {
	// Create test remote codec service
	opts := []Option{
		WithURL(server.URL),
		WithHTTPClient(server.Client()),
		WithNamespace("test"),
		WithMinBytes(32),
	}
	if withDecodeOnly {
		opts = append(opts, WithDecodeOnly())
	}
	// Create test codec (to be used from Go SDK)
	c, err := New(
		opts...,
	)
	require.NoError(t, err)

	c.version = version

	return c
}

func fromFile(t *testing.T) []byte {
	currentPath, err := os.Getwd()
	require.NoError(t, err)
	path := filepath.Join(currentPath+"/testdata", t.Name())
	source, err := os.ReadFile(path)
	require.NoError(t, err)

	return source
}

func toFile(t *testing.T, data []byte) {
	path := filepath.Join("testdata", t.Name())
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		require.NoError(t, err)
	}
}
