package s3

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

type Config struct {
	Session *session.Session
	Bucket  string
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
	return &Driver{
		client:   s3.New(config.Session),
		uploader: s3manager.NewUploader(config.Session),
		downloader: s3manager.NewDownloader(config.Session, func(d *s3manager.Downloader) {
			d.Concurrency = 1 // disable concurrent downloads so that we can write directly to the http response stream
		}),
		bucket:            config.Bucket,
		checksumAlgorithm: s3.ChecksumAlgorithmSha256,
		storageClass:      s3.StorageClassIntelligentTiering,
	}
}

type Driver struct {
	client            *s3.S3
	uploader          *s3manager.Uploader
	downloader        *s3manager.Downloader
	bucket            string
	checksumAlgorithm string
	storageClass      string
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	w := sequentialWriterAt{w: r.Writer}
	if _, err := d.downloader.DownloadWithContext(ctx, &w, &s3.GetObjectInput{
		Bucket:       &d.bucket,
		Key:          aws.String(computeKey(r.Digest)),
		ChecksumMode: &d.checksumAlgorithm,
	}); err != nil {
		return nil, err
	}
	return &storage.GetResponse{}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	result, err := d.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Key:                       aws.String(computeKey(r.Digest)),
		Metadata:                  nil,
		Body:                      r.Data,
		Bucket:                    &d.bucket,
		CacheControl:              nil,
		ChecksumAlgorithm:         &d.checksumAlgorithm,
		ChecksumSHA256:            &r.Digest,
		ContentDisposition:        nil,
		ContentEncoding:           nil,
		ContentType:               nil,
		ExpectedBucketOwner:       nil,
		Expires:                   nil,
		GrantFullControl:          nil,
		GrantRead:                 nil,
		GrantReadACP:              nil,
		ObjectLockLegalHoldStatus: nil,
		ObjectLockMode:            nil,
		ObjectLockRetainUntilDate: nil,
		StorageClass:              &d.storageClass,
		Tagging:                   nil,
	}, s3manager.WithUploaderRequestOptions(request.WithSetRequestHeaders(map[string]string{
		"Content-Length": strconv.FormatUint(r.ContentLength, 10),
	})))
	if err != nil {
		return nil, err
	}

	return &storage.PutResponse{
		Location: result.Location,
	}, nil
}

func computeKey(digest string) string {
	return fmt.Sprintf("blobs/%s", digest)
}
