package codec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

type remotePayload struct {
	// Content of the original payload's Metadata
	Metadata map[string][]byte `json:"metadata"`
	// Number of bytes in the payload Data
	Size uint `json:"size"`
	// Digest of the payload Data, prefixed with the algorithm.
	//
	// E.g. sha256:deadbeef
	Digest string `json:"digest"`
	// URL where the blob was uploaded.
	Location string `json:"location"`
}

type Option interface {
	apply(*Codec) error
}

type applier func(*Codec) error

func (a applier) apply(c *Codec) error {
	return a(c)
}

func WithURL(url string) Option {
	return applier(func(c *Codec) error {
		c.url = url
		return nil
	})
}

func WithMinBytes(bytes uint64) Option {
	return applier(func(c *Codec) error {
		c.minBytes = int(bytes)
		return nil
	})
}

func WithHTTPClient(client *http.Client) Option {
	return applier(func(c *Codec) error {
		c.client = client
		return nil
	})
}

func WithHTTPRoundTripper(rt http.RoundTripper) Option {
	return applier(func(c *Codec) error {
		if c.client == nil {
			return fmt.Errorf("no http client option set")
		}
		c.client.Transport = rt
		return nil
	})
}

func New(opts ...Option) (*Codec, error) {
	c := Codec{
		client:   http.DefaultClient,
		url:      "",
		minBytes: 128,
	}

	for _, opt := range opts {
		if err := opt.apply(&c); err != nil {
			return nil, err
		}
	}

	if c.client == nil {
		return nil, fmt.Errorf("an http client is required")
	}
	if c.url == "" {
		return nil, fmt.Errorf("a remote codec URL is required")
	}

	return &c, nil
}

type Codec struct {
	client *http.Client
	url    string
	// Minimum size of the payload in order to use remote codec
	minBytes int
}

func (c *Codec) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	var (
		ctx    = context.Background()
		result = make([]*common.Payload, len(payloads))
	)

	for i, payload := range payloads {
		if payload.Size() > c.minBytes {
			p, err := c.encodePayload(ctx, payload)
			if err != nil {
				return nil, err
			}
			result[i] = p
		} else {
			result[i] = payload
		}
	}

	return result, nil
}

func (c *Codec) encodePayload(ctx context.Context, payload *common.Payload) (*common.Payload, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		c.url+"/blobs/upload",
		bytes.NewReader(payload.GetData()),
	)
	if err != nil {
		return nil, err
	}

	sha2 := sha256.New()
	sha2.Write(payload.GetData())
	digest := "sha256:" + hex.EncodeToString(sha2.Sum(nil))

	q := req.URL.Query()
	q.Set("digest", digest)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status code %d: %s", resp.StatusCode, respBody)
	}

	result, err := converter.GetDefaultDataConverter().ToPayload(remotePayload{
		Metadata: payload.GetMetadata(),
		Size:     uint(len(payload.GetData())),
		Digest:   digest,
		Location: resp.Header.Get("Location"),
	})
	if err != nil {
		return nil, err
	}
	result.Metadata["temporal.io/remote-codec"] = []byte("v1")

	return result, nil
}

func (c *Codec) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	result := make([]*common.Payload, len(payloads))
	for i, p := range payloads {
		if codecVersion, ok := p.GetMetadata()["temporal.io/remote-codec"]; ok {
			switch string(codecVersion) {
			case "v1":
				decodedPayload, err := c.decodePayload(context.Background(), p)
				if err != nil {
					return nil, err
				}
				result[i] = decodedPayload
			default:
				return nil, fmt.Errorf("unknown version for temporal.io/remote-codec: %s", codecVersion)
			}
		} else {
			result[i] = p
		}
	}
	return result, nil
}

func (c *Codec) decodePayload(ctx context.Context, payload *common.Payload) (*common.Payload, error) {
	var remoteP remotePayload
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &remoteP); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.url+"/blobs/get",
		nil,
	)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Set("digest", remoteP.Digest)
	req.URL.RawQuery = q.Encode()

	// TODO: double check content type
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status code %d: %s", resp.StatusCode, respBody)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &common.Payload{
		Metadata: remoteP.Metadata,
		Data:     b,
	}, nil
}
