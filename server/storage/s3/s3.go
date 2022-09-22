package s3

import (
	"context"
	"errors"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"io"
)

// Config provides all configuration to create the S3 based driver for LPS.
type Config struct {
	Config aws.Config
	Bucket string
}

// A sequentialWriterAt trivially satisfies the [io.WriterAt] interface
// by ignoring the supplied offset and writing bytes to the wrapped w sequentially.
// It is meant to be used with a [s3manager.Downloader] with `Concurrency` set to 1.
type sequentialWriterAt struct {
	w io.Writer
}

func (s *sequentialWriterAt) WriteAt(p []byte, _ int64) (n int, err error) {
	return s.w.Write(p)
}

func New(config *Config) *Driver {
	cli := s3.NewFromConfig(config.Config, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return &Driver{
		client: cli,
		uploader: manager.NewUploader(cli, func(u *manager.Uploader) {
			u.Concurrency = 1 // disable concurrent uploads so we can read directly from the http request body
		}),
		downloader: manager.NewDownloader(cli, func(d *manager.Downloader) {
			d.Concurrency = 1 // disable concurrent downloads so that we can write directly to the http response stream
		}),
		bucket:       config.Bucket,
		storageClass: s3types.StorageClassIntelligentTiering,
	}
}

type Driver struct {
	client       *s3.Client
	uploader     *manager.Uploader
	downloader   *manager.Downloader
	bucket       string
	storageClass s3types.StorageClass
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	w := sequentialWriterAt{w: r.Writer}
	numBytes, err := d.downloader.Download(ctx, &w, &s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    aws.String(r.Key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			err = &storage.ErrBlobNotFound{
				Err: err,
			}
		}
		return nil, err
	}

	return &storage.GetResponse{
		ContentLength: uint64(numBytes),
	}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	_, err := d.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:        &d.bucket,
		Key:           aws.String(r.Key),
		Body:          r.Data,
		ContentLength: int64(r.ContentLength),
		StorageClass:  d.storageClass,
	})
	if err != nil {
		return nil, err
	}

	return &storage.PutResponse{
		Key: r.Key,
	}, nil
}

func (d *Driver) ExistPayload(ctx context.Context, r *storage.ExistRequest) (*storage.ExistResponse, error) {
	_, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &d.bucket,
		Key:    &r.Key,
	})

	exists := true
	if err != nil {
		// I would expect the API to return s3types.NoSuchKey, but that is not the case.
		// This might change in upcoming releases.
		var ae smithy.APIError
		if errors.As(err, &ae) && ae.ErrorCode() == "NotFound" {
			exists = false
		} else {
			return nil, err
		}
	}

	return &storage.ExistResponse{
		Exists: exists,
	}, nil
}

func (d *Driver) Validate(ctx context.Context) error {
	input := &s3.HeadBucketInput{
		Bucket: &d.bucket,
	}
	if _, err := d.client.HeadBucket(ctx, input); err != nil {
		return fmt.Errorf("unable to access S3 bucket '%s'", d.bucket)
	}
	return nil
}
