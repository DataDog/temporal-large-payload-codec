package codec_test

import (
	"context"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/stretchr/testify/require"
	"net/http/httptest"
	"strings"
	"testing"

	codec "github.com/DataDog/temporal-large-payload-codec"
	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"go.temporal.io/api/common/v1"
)

func TestV1Codec(t *testing.T) {
	testCase := []struct {
		name           string
		payload        []*common.Payload
		encodedPayload []*common.Payload
	}{
		{
			name: "data <32 bytes",
			payload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"foo": []byte("bar"),
					},
					Data: []byte("hello world"),
				},
			},
			encodedPayload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"foo": []byte("bar"),
					},
					Data: []byte("hello world"),
				},
			},
		},
		{
			name: "data >32 bytes",
			payload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"foo": []byte("bar"),
						"baz": []byte("qux"),
					},
					Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
				},
			},
			encodedPayload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"encoding":                 []byte("json/plain"),
						"temporal.io/remote-codec": []byte("v1"),
					},
					Data: []byte("{\"metadata\":{\"baz\":\"cXV4\",\"foo\":\"YmFy\"},\"size\":59,\"digest\":\"sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\",\"location\":\"\",\"key\":\"blobs/sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\"}"),
				},
			},
		},
		{
			name: "no metadata",
			payload: []*common.Payload{
				{
					Data: []byte("This message is also longer than the 32 bytes limit!"),
				},
			},
			encodedPayload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"encoding":                 []byte("json/plain"),
						"temporal.io/remote-codec": []byte("v1"),
					},
					Data: []byte("{\"metadata\":null,\"size\":52,\"digest\":\"sha256:62c5b63b2e7bccbddd931c896593b25fbab2ea1c12b0e1fb34ca083536c2c066\",\"location\":\"\",\"key\":\"blobs/sha256:62c5b63b2e7bccbddd931c896593b25fbab2ea1c12b0e1fb34ca083536c2c066\"}"),
				},
			},
		},
	}

	s, c, _ := setUp(t)
	defer s.Close()

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			actualEncodedPayload, err := c.Encode(scenario.payload)
			if err != nil {
				require.NoError(t, err)
			}
			require.Equal(t, scenario.encodedPayload[0].Data, actualEncodedPayload[0].Data)

			actualPayload, err := c.Decode(scenario.encodedPayload)
			require.NoError(t, err)
			require.Equal(t, scenario.payload, actualPayload)
		})
	}
}

func TestV2Codec(t *testing.T) {
	testCase := []struct {
		name           string
		payload        []*common.Payload
		encodedPayload []*common.Payload
	}{
		{
			name: "data <32 bytes",
			payload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"foo": []byte("bar"),
					},
					Data: []byte("hello world"),
				},
			},
			encodedPayload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"foo": []byte("bar"),
					},
					Data: []byte("hello world"),
				},
			},
		},
		{
			name: "data >32 bytes",
			payload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"foo": []byte("bar"),
						"baz": []byte("qux"),
					},
					Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
				},
			},
			encodedPayload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"encoding":                 []byte("json/plain"),
						"temporal.io/remote-codec": []byte("v2"),
					},
					Data: []byte("{\"metadata\":{\"baz\":\"cXV4\",\"foo\":\"YmFy\"},\"size\":59,\"digest\":\"sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\",\"location\":\"\",\"key\":\"blobs/sha256:041ae008aa23e071b5f04ae1b75847c7b135269239833501f0929b212c95935c\"}"),
				},
			},
		},
		{
			name: "no metadata",
			payload: []*common.Payload{
				{
					Data: []byte("This message is also longer than the 32 bytes limit!"),
				},
			},
			encodedPayload: []*common.Payload{
				{
					Metadata: map[string][]byte{
						"encoding":                 []byte("json/plain"),
						"temporal.io/remote-codec": []byte("v2"),
					},
					Data: []byte("{\"metadata\":null,\"size\":52,\"digest\":\"sha256:62c5b63b2e7bccbddd931c896593b25fbab2ea1c12b0e1fb34ca083536c2c066\",\"location\":\"\",\"key\":\"blobs/sha256:62c5b63b2e7bccbddd931c896593b25fbab2ea1c12b0e1fb34ca083536c2c066\"}"),
				},
			},
		},
	}

	s, c, _ := setUp(t)
	defer s.Close()

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			actualEncodedPayload, err := c.Encode(scenario.payload)
			if err != nil {
				require.NoError(t, err)
			}
			require.Equal(t, scenario.encodedPayload, actualEncodedPayload)

			actualPayload, err := c.Decode(scenario.encodedPayload)
			if err != nil {
				require.NoError(t, err)
			}
			require.Equal(t, scenario.payload, actualPayload)

		})
	}
}

func TestDecodeExistingV1Payload(t *testing.T) {
	s, c, d := setUp(t)
	defer s.Close()

	encodedPayload := &common.Payload{
		Metadata: map[string][]byte{
			"encoding":                 []byte("json/plain"),
			"temporal.io/remote-codec": []byte("v1"),
		},
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

func setUp(t *testing.T) (*httptest.Server, *codec.Codec, storage.Driver) {
	// Create test remote codec service
	d := &memory.Driver{}
	s := httptest.NewServer(server.NewHttpHandler(d))

	// Create test codec (to be used from Go SDK)
	c, err := codec.New(
		codec.WithURL(s.URL),
		codec.WithHTTPClient(s.Client()),
		codec.WithMinBytes(32),
	)
	if err != nil {
		require.NoError(t, err)
	}

	return s, c, d
}
