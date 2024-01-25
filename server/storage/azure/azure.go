// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package azure

import (
	"context"
	"fmt"
	"io"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

// Config provides all configuration to create the Azure based driver for LPS.
type Config struct {
	CredOpts   *azidentity.DefaultAzureCredentialOptions
	Container  string
	ServiceURL string
}

type Driver struct {
	client    *azblob.Client
	container string
}

func New(config *Config) (*Driver, error) {
	cred, err := azidentity.NewDefaultAzureCredential(config.CredOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create azure credential: %w", err)
	}
	client, err := azblob.NewClient(config.ServiceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to create azure client: %w", err)
	}

	return &Driver{
		client:    client,
		container: config.Container,
	}, nil
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	resp, err := d.client.DownloadStream(ctx, d.container, r.Key, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			return nil, &storage.ErrBlobNotFound{Err: err}
		}
		return nil, err
	}
	numBytes, err := io.Copy(r.Writer, resp.Body)
	if err != nil {
		return nil, err
	}

	return &storage.GetResponse{
		ContentLength: uint64(numBytes),
	}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	_, err := d.client.UploadStream(ctx, d.container, r.Key, r.Data, nil)
	if err != nil {
		return nil, err
	}

	return &storage.PutResponse{
		Key: r.Key,
	}, nil
}

func (d *Driver) ExistPayload(ctx context.Context, r *storage.ExistRequest) (*storage.ExistResponse, error) {
	exists := true
	_, err := d.client.ServiceClient().NewContainerClient(d.container).NewBlobClient(r.Key).GetProperties(ctx, nil)
	if err != nil {
		if bloberror.HasCode(err, bloberror.BlobNotFound) {
			exists = false
		} else {
			return nil, err
		}
	}

	return &storage.ExistResponse{
		Exists: exists,
	}, nil
}

func (d *Driver) DeletePayload(ctx context.Context, r *storage.DeleteRequest) (*storage.DeleteResponse, error) {
	_, err := d.client.DeleteBlob(ctx, d.container, r.Key, nil)
	if err != nil {
		return nil, err
	}

	return &storage.DeleteResponse{}, nil
}

func (d *Driver) Validate(ctx context.Context) error {
	_, err := d.client.ServiceClient().NewContainerClient(d.container).GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to access Azure container '%s': %s", d.container, err)
	}

	return nil
}
