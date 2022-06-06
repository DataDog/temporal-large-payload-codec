package codec_test

import (
	"net/http/httptest"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/google/go-cmp/cmp"
	"go.temporal.io/api/common/v1"
	"google.golang.org/protobuf/testing/protocmp"

	codec "github.com/DataDog/temporal-large-payload-codec"
	"github.com/DataDog/temporal-large-payload-codec/server"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
)

func TestCodec(t *testing.T) {
	// Create test remote codec service
	s := httptest.NewServer(server.NewHttpHandler(&memory.Driver{}))
	defer s.Close()
	// Create test codec (to be used from Go SDK)
	c, err := codec.New(
		codec.WithURL(s.URL),
		codec.WithHTTPClient(s.Client()),
		codec.WithMinBytes(32),
	)
	if err != nil {
		t.Fatal(err)
	}

	testPayloads := []*common.Payload{
		{
			Metadata: map[string][]byte{
				"foo": []byte("bar"),
			},
			Data: []byte("hello world"),
		},
		{
			Metadata: map[string][]byte{
				"foo": []byte("bar"),
				"baz": []byte("qux"),
			},
			Data: []byte("this is a longer message blah blah blah blah blah blah blah"),
		},
		{
			Data: []byte("hello dave"),
		},
	}

	// Encode some test payloads
	encodeResult, err := c.Encode(testPayloads)
	if err != nil {
		t.Fatal(err)
	}

	// Validate that only large payloads are encoded
	for i, p := range encodeResult {
		if testPayloads[i].Size() > 32 {
			if _, ok := p.GetMetadata()["temporal.io/remote-codec"]; !ok {
				t.Errorf("expected payload %d to trigger remote codec, got: %s", i, proto.MarshalTextString(p))
			}
		} else if diff := cmp.Diff(testPayloads[i], p, protocmp.Transform()); diff != "" {
			t.Errorf("expected no diff for test payload %d, got:\n%s", i, diff)
		}
	}

	decodeResult, err := c.Decode(encodeResult)
	if err != nil {
		t.Fatal(err)
	}

	for i, testPayload := range testPayloads {
		if diff := cmp.Diff(testPayload, decodeResult[i], protocmp.Transform()); diff != "" {
			t.Errorf("expected no diff for test payload, got: %s", diff)
		}
	}
}
