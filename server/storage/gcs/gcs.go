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

func New(ctx context.Context, bucket string) *Driver {
	client, err := gcs.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	return &Driver{
		client: client,
		bucket: bucket,
	}
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	rc, err := d.client.Bucket(d.bucket).Object(storage.ComputeKey(r.Digest)).NewReader(ctx)
	if err != nil {
		if errors.Is(err, gcs.ErrObjectNotExist) {
			return nil, &storage.ErrBlobNotFound{Err: err}
		}
		return nil, err
	}
	defer rc.Close()

	numBytes, err := io.Copy(r.Writer, rc)
	if err != nil {
		return nil, err
	}

	return &storage.GetResponse{
		ContentLength: uint64(numBytes),
	}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	o := d.client.Bucket(d.bucket).Object(storage.ComputeKey(r.Digest))

	// Upload an object with storage.Writer.
	wc := o.NewWriter(ctx)

	if _, err := io.Copy(wc, r.Data); err != nil {
		return nil, fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return nil, fmt.Errorf("Writer.Close: %v", err)
	}
	return &storage.PutResponse{}, nil
}

func (d *Driver) Validate(ctx context.Context) error {
	bucketHandle := d.client.Bucket(d.bucket)
	if _, err := bucketHandle.Attrs(ctx); err != nil {
		return fmt.Errorf("unable to access GCS bucket '%s'", d.bucket)
	}
	return nil
}
