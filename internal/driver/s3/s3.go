package s3

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/DataDog/temporal-large-payload-codec/internal/driver"
)

type Config struct {
	Session *session.Session
	Bucket  string
}

func New(config *Config) *Driver {
	return &Driver{
		client:   s3.New(config.Session),
		uploader: s3manager.NewUploader(config.Session),
		bucket:   config.Bucket,
	}
}

type Driver struct {
	client   *s3.S3
	uploader *s3manager.Uploader
	bucket   string
}

func (d *Driver) GetPayload(ctx context.Context, request *driver.GetRequest) (*driver.GetResponse, error) {
	// TODO implement me
	panic("implement me")
}

func (d *Driver) PutPayload(ctx context.Context, request *driver.PutRequest) (*driver.PutResponse, error) {
	result, err := d.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		ACL:              nil,
		Body:             request.Payload,
		Bucket:           &d.bucket,
		BucketKeyEnabled: nil,
		CacheControl:     nil,
		// ChecksumAlgorithm:         s3.ChecksumAlgorithmSha256,
		ChecksumSHA256:            &d.bucket, // TODO
		ContentDisposition:        nil,
		ContentEncoding:           nil,
		ContentType:               nil,
		ExpectedBucketOwner:       nil,
		Expires:                   nil,
		GrantFullControl:          nil,
		GrantRead:                 nil,
		GrantReadACP:              nil,
		GrantWriteACP:             nil,
		Key:                       nil,
		Metadata:                  nil,
		ObjectLockLegalHoldStatus: nil,
		ObjectLockMode:            nil,
		ObjectLockRetainUntilDate: nil,
		RequestPayer:              nil,
		SSECustomerAlgorithm:      nil,
		SSECustomerKey:            nil,
		SSECustomerKeyMD5:         nil,
		SSEKMSEncryptionContext:   nil,
		SSEKMSKeyId:               nil,
		ServerSideEncryption:      nil,
		StorageClass:              nil,
		Tagging:                   nil,
		WebsiteRedirectLocation:   nil,
	})
	if err != nil {
		return nil, err
	}

	return &driver.PutResponse{
		Location: result.Location,
	}, nil
}
