// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package azure

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/orlangure/gnomock"
	"github.com/stretchr/testify/require"
)

const (
	defaultAzuriteVersion  = "3.21.0"
	defaultAzuriteUsername = "devstoreaccount1"
	defaultAzuritePassword = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	testBucketName         = "lps-test-bucket"
	BlobPort               = "blob"
)

func TestAzureDriver(t *testing.T) {
	_, set := os.LookupEnv("ACT")
	if set {
		t.Skip("Skipping this test when running within act")
	}

	config, closerFunc := setUp(t)
	defer closerFunc()

	// Build the driver. Need to create the client explicitly to be able to use NewSharedKeyCredential opposed to NewDefaultAzureCredential
	credential, err := azblob.NewSharedKeyCredential(defaultAzuriteUsername, defaultAzuritePassword)
	require.NoError(t, err)
	client, err := azblob.NewClientWithSharedKeyCredential(config.ServiceURL, credential, nil)
	require.NoError(t, err)
	driver := &Driver{
		client:    client,
		container: config.Container,
	}
	require.NoError(t, err)

	buf := bytes.Buffer{}
	ctx := context.Background()

	// Check missing payload
	resp, err := driver.ExistPayload(ctx, &storage.ExistRequest{Key: "sha256:foobar"})
	require.NoError(t, err)
	require.False(t, resp.Exists)

	// Get missing payload
	_, err = driver.GetPayload(ctx, &storage.GetRequest{Key: "sha256:foobar", Writer: &buf})
	var blobNotFound *storage.ErrBlobNotFound
	require.True(t, errors.As(err, &blobNotFound))
	require.Equal(t, buf.Len(), 0)

	// Put a payload
	testPayloadBytes := []byte("hello world")
	putResponse, err := driver.PutPayload(ctx, &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Key:           "blobs/sha256:test",
		Digest:        "sha256:test",
		ContentLength: uint64(len(testPayloadBytes)),
	})
	require.NoError(t, err)
	require.NotEmpty(t, putResponse.Key)

	// Check payload exists
	resp, err = driver.ExistPayload(ctx, &storage.ExistRequest{Key: putResponse.Key})
	require.NoError(t, err)
	require.True(t, resp.Exists)

	// Get the payload back out and compare to original bytes
	_, err = driver.GetPayload(ctx, &storage.GetRequest{Key: putResponse.Key, Writer: &buf})
	require.NoError(t, err)

	// Read the data
	b, err := io.ReadAll(&buf)
	require.NoError(t, err)
	require.Equal(t, b, testPayloadBytes)

	// Delete the payload
	_, err = driver.DeletePayload(ctx, &storage.DeleteRequest{Key: putResponse.Key})
	require.NoError(t, err)

	// Ensure the payload was deleted
	resp, err = driver.ExistPayload(ctx, &storage.ExistRequest{Key: putResponse.Key})
	require.False(t, resp.Exists)

	time.Sleep(1 * time.Second)
}

func setUp(t *testing.T) (Config, func()) {
	p := AzuritePreset(
		WithVersion("3.21.0"),
		WithBuckets([]string{testBucketName}),
	)
	opts := []gnomock.Option{
		gnomock.WithDebugMode(),
		gnomock.WithCommand("azurite", "-d", "debug.log", "--blobHost", "0.0.0.0"),
	}
	container, err := gnomock.Start(p, opts...)
	require.NoError(t, err)
	closer := func() { _ = gnomock.Stop(container) }

	require.NoError(t, err)

	url := fmt.Sprintf("http://%s/devstoreaccount1", container.Address(BlobPort))
	config := Config{
		Container:  testBucketName,
		ServiceURL: url,
	}
	return config, closer
}

func AzuritePreset(opts ...Option) gnomock.Preset {
	a := &Azurite{}

	for _, opt := range opts {
		opt(a)
	}

	return a
}

type Option func(*Azurite)

func WithVersion(version string) Option {
	return func(a *Azurite) {
		a.Version = version
	}
}

func WithBuckets(buckets []string) Option {
	return func(a *Azurite) {
		a.Buckets = buckets
	}
}

type Azurite struct {
	Version string
	Buckets []string
}

func (a *Azurite) Image() string {
	return fmt.Sprintf("mcr.microsoft.com/azure-storage/azurite:%s", a.Version)
}

func (a *Azurite) Ports() gnomock.NamedPorts {
	return gnomock.NamedPorts{
		BlobPort: {Protocol: "tcp", Port: 10000},
	}
}

func (a *Azurite) Options() []gnomock.Option {
	a.setDefaults()

	opts := []gnomock.Option{
		gnomock.WithInit(a.initf()),
	}

	return opts
}

func (a *Azurite) setDefaults() {
	if a.Version == "" {
		a.Version = defaultAzuriteVersion
	}
}

func (a *Azurite) initf() gnomock.InitFunc {
	return func(ctx context.Context, c *gnomock.Container) error {
		credential, err := azblob.NewSharedKeyCredential("devstoreaccount1", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
		if err != nil {
			return err
		}
		url := fmt.Sprintf("http://%s/devstoreaccount1", c.Address(BlobPort))
		client, err := azblob.NewClientWithSharedKeyCredential(url, credential, nil)
		if err != nil {
			return err
		}

		for _, bucket := range a.Buckets {
			_, err = client.CreateContainer(ctx, bucket, nil)
			if err != nil {
				return err
			}
		}
		return nil
	}
}
