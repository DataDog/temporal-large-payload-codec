package s3

import (
	"context"
	"fmt"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"io"
)

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
	cli := s3.NewFromConfig(config.Config)
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
	var err error

	span, _ := tracer.SpanFromContext(ctx)
	span.SetOperationName("GetPayload - S3")
	defer span.Finish(tracer.WithError(err))

	{
		bucket, key := d.bucket, computeKey(r.Digest)

		s3opSpan := tracer.StartSpan("AWS S3 GET", tracer.ChildOf(span.Context()))
		s3opSpan.SetTag("bucket", bucket)
		s3opSpan.SetTag("key", key)
		defer s3opSpan.Finish(tracer.WithError(err))

		w := sequentialWriterAt{w: r.Writer}
		if downloadedByteCount, err := d.downloader.Download(ctx, &w, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		}); err != nil {
			return nil, err
		} else {
			s3opSpan.SetTag("bytes", downloadedByteCount)
		}
	}

	return &storage.GetResponse{}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	var (
		err    error
		result *manager.UploadOutput
	)

	span, _ := tracer.SpanFromContext(ctx)
	span.SetOperationName("PutPayload - S3")
	defer span.Finish(tracer.WithError(err))

	{
		bucket, key := d.bucket, computeKey(r.Digest)

		s3opSpan := tracer.StartSpan("AWS S3 PUT", tracer.ChildOf(span.Context()))
		s3opSpan.SetTag("bucket", bucket)
		s3opSpan.SetTag("key", key)
		defer s3opSpan.Finish(tracer.WithError(err))

		result, err = d.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			Body:          r.Data,
			ContentLength: int64(r.ContentLength),
			StorageClass:  d.storageClass,
		})

		if err != nil {
			return nil, err
		}
	}

	return &storage.PutResponse{
		Location: result.Location,
	}, nil
}

func computeKey(digest string) string {
	return fmt.Sprintf("blobs/%s", digest)
}
