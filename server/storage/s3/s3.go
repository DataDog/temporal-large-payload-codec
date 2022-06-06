package s3

import (
	"context"
	"fmt"

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

func (d *Driver) GetPayload(ctx context.Context, request *storage.GetRequest) (*storage.GetResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d *Driver) PutPayload(ctx context.Context, request *storage.PutRequest) (*storage.PutResponse, error) {
	result, err := d.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Key:                       computeKey(request.Digest),
		Metadata:                  nil,
		Body:                      request.Payload,
		Bucket:                    &d.bucket,
		ACL:                       nil,
		CacheControl:              nil,
		ChecksumAlgorithm:         &d.checksumAlgorithm,
		ChecksumSHA256:            &request.Digest,
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
	})
	if err != nil {
		return nil, err
	}

	return &storage.PutResponse{
		Location: result.Location,
	}, nil
}

func computeKey(digest string) *string {
	result := fmt.Sprintf("blobs/%s", digest)
	return &result
}
