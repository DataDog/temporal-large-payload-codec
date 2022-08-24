package codec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

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

// WithURL sets the endpoint for the remote payload storage service.
func WithURL(url string) Option {
	return applier(func(c *Codec) error {
		c.url = url
		return nil
	})
}

// WithMinBytes configures the minimum size of an event payload needed to trigger
// encoding using the large payload codec. Any payload smaller than this value
// will be transparently persisted in workflow history.
//
// The default value is 128000, or 128KB.
//
// Setting this too low can lead to degraded performance, since decoding requires
// an additional network round trip per payload. This can add up quickly when
// replaying a workflow with a large number of events.
//
// According to https://docs.temporal.io/workflows, the hard limit for workflow
// history size is 50k events and 50MB. A workflow with exactly 50k events can
// therefore in theory have an average event payload size of 1048 bytes.
//
// In practice this worst case is very unlikely, since common workflow event
// types such as WorkflowTaskScheduled or WorkflowTaskCompleted do not include user
// defined payloads. If we estimate that one quarter of events have payloads just
// below the cutoff, then we can calculate how many events total would fit in
// one workflow's history (the point before which we must call ContinueAsNew):
//
//	AverageNonUserTaskBytes = 1024 (generous estimate for events like WorkflowTaskScheduled)
//	CodecMinBytes = 128_000
//	AverageEventBytes = (AverageNonUserTaskBytes * 3 + CodecMinBytes) / 4 = 32_768
//	MaxHistoryEventBytes = 50_000_000
//	MaxHistoryEventCount = MaxHistoryEventBytes / AverageEventBytes = 1525
func WithMinBytes(bytes uint32) Option {
	return applier(func(c *Codec) error {
		c.minBytes = int(bytes)
		return nil
	})
}

// WithHTTPClient sets a custom http.Client.
//
// If unspecified, http.DefaultClient will be used.
func WithHTTPClient(client *http.Client) Option {
	return applier(func(c *Codec) error {
		c.client = client
		return nil
	})
}

// WithHTTPRoundTripper sets custom Transport on the http.Client.
//
// This may be used to implement use cases including authentication or tracing.
func WithHTTPRoundTripper(rt http.RoundTripper) Option {
	return applier(func(c *Codec) error {
		if c.client == nil {
			return fmt.Errorf("no http client option set")
		}
		c.client.Transport = rt
		return nil
	})
}

// New instantiates a Codec. WithURL is a required option.
//
// An error may be returned if incompatible options are configured or if a
// connection to the remote payload storage service cannot be established.
func New(opts ...Option) (*Codec, error) {
	c := Codec{
		client: http.DefaultClient,
		url:    "",
		// 128KB happens to be the lower bound for blobs eligible for AWS S3
		// Intelligent-Tiering:
		// https://aws.amazon.com/s3/storage-classes/intelligent-tiering/
		minBytes: 128_000,
	}

	for _, opt := range opts {
		if err := opt.apply(&c); err != nil {
			return nil, err
		}
	}

	// Check for required attributes
	if c.client == nil {
		return nil, fmt.Errorf("an http client is required")
	}
	if c.url == "" {
		return nil, fmt.Errorf("a remote codec URL is required")
	}
	// Validate URL and set version
	u, err := url.Parse(c.url)
	if err != nil {
		return nil, err
	}
	// Set v1 path
	u.Path = "v1"
	c.url = u.String()
	// Check connectivity
	u.Path = path.Join(u.Path, "health/head")
	resp, err := c.client.Head(u.String())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got status code %d from storage service at %s", resp.StatusCode, u)
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
		c.url,
		bytes.NewReader(payload.GetData()),
	)
	if err != nil {
		return nil, err
	}
	req.URL.Path = path.Join(req.URL.Path, "blobs/put")

	sha2 := sha256.New()
	sha2.Write(payload.GetData())
	digest := "sha256:" + hex.EncodeToString(sha2.Sum(nil))

	q := req.URL.Query()
	q.Set("digest", digest)
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(payload.GetData()))

	// Set metadata header
	md, err := json.Marshal(payload.GetMetadata())
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Temporal-Metadata", base64.StdEncoding.EncodeToString(md))

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
		c.url,
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.URL.Path = path.Join(req.URL.Path, "blobs/get")

	q := req.URL.Query()
	q.Set("digest", remoteP.Digest)
	req.URL.RawQuery = q.Encode()

	// TODO: double check content type
	req.Header.Set("Content-Type", "application/octet-stream")
	// TODO: we temporarily need this because we aren't checking object metadata on the server
	req.Header.Set("X-Payload-Expected-Content-Length", strconv.FormatUint(uint64(remoteP.Size), 10))

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
	if uint(len(b)) != remoteP.Size {
		return nil, fmt.Errorf("wanted object of size %d, got %d", remoteP.Size, len(b))
	}
	// TODO: check digest as well?

	return &common.Payload{
		Metadata: remoteP.Metadata,
		Data:     b,
	}, nil
}
