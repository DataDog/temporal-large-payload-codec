package s3

import (
	"context"
	"fmt"
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

func New(config *Config) *Driver {
	return &Driver{
		client:            s3.New(config.Session),
		uploader:          s3manager.NewUploader(config.Session),
		bucket:            config.Bucket,
		checksumAlgorithm: s3.ChecksumAlgorithmSha256,
		storageClass:      s3.StorageClassIntelligentTiering,
	}
}

type Driver struct {
	client            *s3.S3
	uploader          *s3manager.Uploader
	bucket            string
	checksumAlgorithm string
	storageClass      string
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	// TODO implement me
	panic("implement me")
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
