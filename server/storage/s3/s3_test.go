// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package s3

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
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/localstack"
	"github.com/stretchr/testify/require"
)

func TestS3Driver(t *testing.T) {
	_, set := os.LookupEnv("ACT")
	if set {
		t.Skip("Skipping this test when running within act")
	}

	awsConfig, closerFunc := setUp(t)
	defer closerFunc()

	config := Config{
		Config: awsConfig,
		Bucket: "lps-test",
	}

	s3Driver := New(&config)
	buf := bytes.Buffer{}
	ctx := context.Background()

	// Check missing payload
	resp, err := s3Driver.ExistPayload(ctx, &storage.ExistRequest{Key: "sha256:foobar"})
	require.NoError(t, err)
	require.False(t, resp.Exists)

	// Get missing payload
	_, err = s3Driver.GetPayload(ctx, &storage.GetRequest{Key: "sha256:foobar", Writer: &buf})
	var blobNotFound *storage.ErrBlobNotFound
	require.True(t, errors.As(err, &blobNotFound))
	require.Equal(t, buf.Len(), 0)

	// Put a payload
	testPayloadBytes := []byte("hello world")
	putResponse, err := s3Driver.PutPayload(ctx, &storage.PutRequest{
		Data:          bytes.NewReader(testPayloadBytes),
		Key:           "blobs/sha256:test",
		Digest:        "sha256:test",
		ContentLength: uint64(len(testPayloadBytes)),
	})
	require.NoError(t, err)
	require.NotEmpty(t, putResponse.Key)

	// Check payload exists
	resp, err = s3Driver.ExistPayload(ctx, &storage.ExistRequest{Key: putResponse.Key})
	require.NoError(t, err)
	require.True(t, resp.Exists)

	// Get the payload back out and compare to original bytes
	_, err = s3Driver.GetPayload(ctx, &storage.GetRequest{Key: putResponse.Key, Writer: &buf})
	require.NoError(t, err)

	// Read the data
	b, err := io.ReadAll(&buf)
	require.NoError(t, err)
	require.Equal(t, b, testPayloadBytes)

	// Delete the payload
	_, err = s3Driver.DeletePayload(ctx, &storage.DeleteRequest{Key: putResponse.Key})
	require.NoError(t, err)

	// Ensure the payload was deleted
	resp, err = s3Driver.ExistPayload(ctx, &storage.ExistRequest{Key: putResponse.Key})
	require.False(t, resp.Exists)

	time.Sleep(1 * time.Second)
}

func setUp(t *testing.T) (aws.Config, func()) {
	// for localstack permissions can be set to whatever
	_ = os.Setenv("AWS_ACCESS_KEY_ID", "a")
	_ = os.Setenv("AWS_SECRET_ACCESS_KEY", "b")
	_ = os.Setenv("AWS_SESSION_TOKEN", "c")

	p := localstack.Preset(
		localstack.WithServices(localstack.S3),
		localstack.WithS3Files("./testdata"),
		localstack.WithVersion("1.1.0"),
	)
	container, err := gnomock.Start(p, gnomock.WithDebugMode())
	require.NoError(t, err)
	closer := func() { _ = gnomock.Stop(container) }

	awsEndpoint := fmt.Sprintf("http://%s/", container.Address(localstack.APIPort))
	awsRegion := "us-east-1"

	customResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if awsEndpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           awsEndpoint,
				SigningRegion: awsRegion,
			}, nil
		}

		// returning EndpointNotFoundError will allow the service to fallback to its default resolution
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	ctx := context.Background()
	awsConfig, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(awsRegion),
		config.WithEndpointResolver(customResolver),
	)
	require.NoError(t, err)
	return awsConfig, closer
}
