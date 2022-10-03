package codec

import (
	"context"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"go.temporal.io/api/common/v1"
)

const (
	updateEncodedPayload = false
)

func TestV1Codec(t *testing.T) {
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
				Data: []byte("hello world"),
			},
		},
		{
			name: "payload exceeds max byte limit",
			payload: common.Payload{
				Metadata: map[string][]byte{
					"foo": []byte("bar"),
					"baz": []byte("qux"),
				},
				Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
			},
			encodedPayload: common.Payload{

				Metadata: map[string][]byte{
					"encoding":                 []byte("json/plain"),
					"temporal.io/remote-codec": []byte("v1"),
				},
				Data: []byte("{\"metadata\":{\"baz\":\"cXV4\",\"foo\":\"YmFy\"},\"size\":59,\"digest\":\"sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\",\"key\":\"blobs/sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\"}"),
			},
		},
	}

	s, c, _ := setUp(t, "v1")
	defer s.Close()

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			actualEncodedPayload, err := c.Encode([]*common.Payload{&scenario.payload})
			if err != nil {
				require.NoError(t, err)
			}

			if updateEncodedPayload {
				toFile(t, actualEncodedPayload[0].Data)
			}

			// load the encoded payload from file
			scenario.encodedPayload.Data = fromFile(t)

			require.Equal(t, &scenario.encodedPayload, actualEncodedPayload[0])

			actualPayload, err := c.Decode([]*common.Payload{&scenario.encodedPayload})
			if err != nil {
				require.NoError(t, err)
			}
			require.Equal(t, &scenario.payload, actualPayload[0])
		})
	}
}

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
			if err != nil {
				require.NoError(t, err)
			}

			if updateEncodedPayload {
				toFile(t, actualEncodedPayload[0].Data)
			}

			// load the encoded payload from file
			scenario.encodedPayload.Data = fromFile(t)

			require.Equal(t, &scenario.encodedPayload, actualEncodedPayload[0])

			actualPayload, err := c.Decode([]*common.Payload{&scenario.encodedPayload})
			if err != nil {
				require.NoError(t, err)
			}
			require.Equal(t, &scenario.payload, actualPayload[0])
		})
	}
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

func TestDecodeExistingV1Payload(t *testing.T) {
	s, c, d := setUp(t, "v1")
	defer s.Close()

	encodedPayload := &common.Payload{
		Metadata: map[string][]byte{
			"encoding":                 []byte("json/plain"),
			"temporal.io/remote-codec": []byte("v1"),
		},
		// v1 data does not contain a key field, but only the digest. We are making sure this can still be read
		Data: []byte("{\"metadata\":{\"baz\":\"cXV4\",\"foo\":\"YmFy\"},\"size\":59,\"digest\":\"sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\",\"location\":\"\"}"),
	}

	expectedPayload := &common.Payload{
		Metadata: map[string][]byte{
			"foo": []byte("bar"),
			"baz": []byte("qux"),
		},
		Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
	}

	put := storage.PutRequest{
		Key:           "blobs/sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c",
		Digest:        "sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c",
		ContentLength: uint64(len(string(expectedPayload.Data))),
		Data:          strings.NewReader(string(expectedPayload.Data)),
	}
	_, err := d.PutPayload(context.Background(), &put)
	require.NoError(t, err)

	actualPayload, err := c.Decode([]*common.Payload{encodedPayload})
	require.NoError(t, err)
	require.Equal(t, []*common.Payload{expectedPayload}, actualPayload)
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
}

func setUp(t *testing.T, version string) (*httptest.Server, *Codec, storage.Driver) {
	// Create test remote codec service
	d := &memory.Driver{}
	s := httptest.NewServer(server.NewHttpHandler(d))

	// Create test codec (to be used from Go SDK)
	c, err := New(
		WithURL(s.URL),
		WithHTTPClient(s.Client()),
		WithNamespace("test"),
		WithMinBytes(32),
	)
	require.NoError(t, err)

	c.version = version

	return s, c, d
}

func fromFile(t *testing.T) []byte {
	path := filepath.Join("testdata", t.Name())
	source, err := os.ReadFile(path)
	if err != nil {
		require.NoError(t, err)
	}
	return source
}

func toFile(t *testing.T, data []byte) {
	path := filepath.Join("testdata", t.Name())
	err := os.WriteFile(path, data, 0644)
	if err != nil {
		require.NoError(t, err)
	}
}
