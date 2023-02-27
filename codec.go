// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package codec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

const (
	remoteCodecName = "temporal.io/remote-codec"
)

type Codec struct {
	// client is the HTTP client used for talking to the LPS server.
	client *http.Client
	// url is the base URL of the LPS server.
	url *url.URL
	// version is the LPS API version (v1 or v2).
	version string
	// minBytes is the minimum size of the payload in order to use remote codec.
	minBytes int
	// namespace is the Temporal namespace the client using this codec is connected to.
	namespace string
}

type keyResponse struct {
	Key string `json:"key"`
}

type remotePayload struct {
	// Content of the original payload's Metadata.
	Metadata map[string][]byte `json:"metadata"`
	// Number of bytes in the payload Data.
	Size uint `json:"size"`
	// Digest of the payload Data, prefixed with the algorithm, e.g. sha256:deadbeef.
	Digest string `json:"digest"`
	// The key to retrieve the payload from remote storage.
	Key string `json:"key"`
}

type Option interface {
	apply(*Codec) error
}

type applier func(*Codec) error

func (a applier) apply(c *Codec) error {
	return a(c)
}

// WithURL sets the endpoint for the remote payload storage service.
// This option is mandatory.
func WithURL(u string) Option {
	return applier(func(c *Codec) error {
		lpsURL, err := url.Parse(u)
		if err != nil {
			return errors.New("invalid remote codec URL")
		}
		c.url = lpsURL
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

// WithNamespace sets the Temporal namespace the client using this codec is connected to.
// This option is mandatory.
func WithNamespace(namespace string) Option {
	return applier(func(c *Codec) error {
		c.namespace = namespace
		return nil
	})
}

// WithVersion sets the version of the LPS API to use.
func WithVersion(version string) Option {
	return applier(func(c *Codec) error {
		c.version = version
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

	if c.namespace == "" {
		return nil, fmt.Errorf("a namespace is required")
	}

	// Check for required attributes
	if c.client == nil {
		return nil, fmt.Errorf("an http client is required")
	}
	if c.url == nil {
		return nil, fmt.Errorf("a remote codec URL is required")
	}

	if c.version == "" {
		c.version = "v2"
	}

	if c.version != "v2" {
		return nil, fmt.Errorf("invalid codec version: %s", c.version)
	}

	// Check connectivity
	headURL := c.url.JoinPath(c.version, "health", "head")
	resp, err := c.client.Head(headURL.String())
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("got status code %d from storage service at %s", resp.StatusCode, headURL)
	}

	return &c, nil
}

func (c *Codec) Encode(payloads []*common.Payload) ([]*common.Payload, error) {
	var (
		ctx    = context.Background()
		result = make([]*common.Payload, len(payloads))
	)

	for i, payload := range payloads {
		if payload.Size() > c.minBytes {
			encodePayload, err := c.encodePayload(ctx, payload)
			if err != nil {
				return nil, err
			}
			result[i] = encodePayload
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
		c.url.JoinPath(c.version).String(),
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
	q.Set("namespace", c.namespace)
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

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned status code %d: %s", resp.StatusCode, respBody)
	}

	var key keyResponse
	if err := json.Unmarshal(respBody, &key); err != nil {
		return nil, fmt.Errorf("unable to unmarshal put response: %w", err)
	}

	result, err := converter.GetDefaultDataConverter().ToPayload(remotePayload{
		Metadata: payload.GetMetadata(),
		Size:     uint(len(payload.GetData())),
		Digest:   digest,
		Key:      key.Key,
	})
	if err != nil {
		return nil, err
	}
	result.Metadata[remoteCodecName] = []byte(c.version)

	return result, nil
}

func (c *Codec) Decode(payloads []*common.Payload) ([]*common.Payload, error) {
	result := make([]*common.Payload, len(payloads))
	for i, payload := range payloads {
		if codecVersion, ok := payload.GetMetadata()[remoteCodecName]; ok {
			switch string(codecVersion) {
			case "v1", "v2":
				decodedPayload, err := c.decodePayload(context.Background(), payload, string(codecVersion))
				if err != nil {
					return nil, err
				}
				result[i] = decodedPayload
			default:
				return nil, fmt.Errorf("unknown version for %s: %s", remoteCodecName, codecVersion)
			}
		} else {
			result[i] = payload
		}
	}
	return result, nil
}

func (c *Codec) decodePayload(ctx context.Context, payload *common.Payload, version string) (*common.Payload, error) {
	var remoteP remotePayload
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &remoteP); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.url.JoinPath(version).String(),
		nil,
	)
	if err != nil {
		return nil, err
	}
	req.URL.Path = path.Join(req.URL.Path, "blobs/get")

	q := req.URL.Query()
	if version == "v1" {
		q.Set("digest", remoteP.Digest)
	}
	if version == "v2" {
		q.Set("key", remoteP.Key)
	}
	req.URL.RawQuery = q.Encode()

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

	sha2 := sha256.New()
	tee := io.TeeReader(resp.Body, sha2)
	b, err := io.ReadAll(tee)
	if err != nil {
		return nil, err
	}

	if uint(len(b)) != remoteP.Size {
		return nil, fmt.Errorf("wanted object of size %d, got %d", remoteP.Size, len(b))
	}

	checkSum := hex.EncodeToString(sha2.Sum(nil))
	if fmt.Sprintf("sha256:%s", checkSum) != remoteP.Digest {
		return nil, fmt.Errorf("wanted object sha %s, got %s", remoteP.Digest, checkSum)
	}

	return &common.Payload{
		Metadata: remoteP.Metadata,
		Data:     b,
	}, nil
}
