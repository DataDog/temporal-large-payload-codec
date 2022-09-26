package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	gcs "cloud.google.com/go/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

type Driver struct {
	client *gcs.Client
	bucket string
}

func New(ctx context.Context, bucket string) (*Driver, error) {
	client, err := gcs.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to create gcs client: %w", err)
	}
	return &Driver{
		client: client,
		bucket: bucket,
	}, nil
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	reader, err := d.client.Bucket(d.bucket).Object(storage.ComputeKey(r.Digest)).NewReader(ctx)
	if err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) {
			return nil, &storage.ErrBlobNotFound{Err: err}
		}
		return nil, err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("unable to close bucket reader: %v", err)
		}
	}()

	numBytes, err := io.Copy(r.Writer, reader)
	if err != nil {
		return nil, err
	}

	return &storage.GetResponse{
		ContentLength: uint64(numBytes),
	}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	path := storage.ComputeKey(r.Digest)
	o := d.client.Bucket(d.bucket).Object(path)

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)

	if _, err := io.Copy(wc, r.Data); err != nil {
		return nil, fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return nil, fmt.Errorf("Writer.Close: %v", err)
	}
	return &storage.PutResponse{
		Location: fmt.Sprintf("gs://%s/%s", d.bucket, path),
	}, nil
}

func (d *Driver) DeletePayload(ctx context.Context, request *storage.DeleteRequest) (*storage.DeleteResponse, error) {
	// For massive deletions we may want to resort to updating Object Lifecycle Policies to 
	// 0 days
	for _, key := range request.Keys {
		o := d.client.Bucket(d.bucket).Object(key)
		if err := o.Delete(ctx); err != nil {
			return nil , err
		}
	}

	return &storage.DeleteResponse{}, nil
}

func (d *Driver) Validate(ctx context.Context) error {
	bucketHandle := d.client.Bucket(d.bucket)
	if _, err := bucketHandle.Attrs(ctx); err != nil {
		return fmt.Errorf("unable to access GCS bucket '%s': %s", d.bucket, err)
	}
	return nil
}
