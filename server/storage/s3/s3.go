package s3

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	aws2 "github.com/aws/aws-sdk-go-v2/aws"
	aws2s3manager "github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	aws2s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	aws2s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"

	"io"
	"strings"

	"github.com/DataDog/temporal-large-payload-codec/server/storage"
)

const (
	awsDownloadChecksumMode = "ENABLED"
	sha256DigestName        = "sha256"
)

type Config struct {
	Config aws2.Config
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
	cli := aws2s3.NewFromConfig(config.Config)
	return &Driver{
		client: cli,
		uploader: aws2s3manager.NewUploader(cli, func(u *aws2s3manager.Uploader) {
			u.Concurrency = 1 // disable concurrent uploads
		}),
		downloader: aws2s3manager.NewDownloader(cli, func(d *aws2s3manager.Downloader) {
			d.Concurrency = 1 // disable concurrent downloads so that we can write directly to the http response stream
		}),
		bucket:       config.Bucket,
		storageClass: aws2s3types.StorageClassIntelligentTiering,
	}
}

type Driver struct {
	client     *aws2s3.Client
	uploader   *aws2s3manager.Uploader
	downloader *aws2s3manager.Downloader
	bucket     string
	//checksumAlgorithm string
	storageClass aws2s3types.StorageClass
}

func (d *Driver) GetPayload(ctx context.Context, r *storage.GetRequest) (*storage.GetResponse, error) {
	w := sequentialWriterAt{w: r.Writer}
	if downloadedBytes, err := d.downloader.Download(ctx, &w, &aws2s3.GetObjectInput{
		Bucket: &d.bucket,
		Key:    aws2.String(computeKey(r.Digest)),
	}); err != nil {
		log.Printf("failed GET:%v", err)
		return nil, err
	} else {
		log.Printf("succeeded GET:%v", downloadedBytes)
	}
	return &storage.GetResponse{}, nil
}

func (d *Driver) PutPayload(ctx context.Context, r *storage.PutRequest) (*storage.PutResponse, error) {
	result, err := d.uploader.Upload(ctx, &aws2s3.PutObjectInput{
		Bucket:        &d.bucket,
		Key:           aws2.String(computeKey(r.Digest)),
		Body:          r.Data,
		ContentLength: int64(r.ContentLength),
	})
	if err != nil {
		log.Printf("failed PUT:%v", err)
		return nil, err
	} else {
		log.Printf("succeeded PUT:%v", result)
	}

	return &storage.PutResponse{
		Location: result.Location,
	}, nil
}

// Compute a base64-encoded representation of a sha256 digest.
// see `x-amz-checksum-sha256` in https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
func computeAwsDigest(digestSpec string) (string, error) {
	tokens := strings.Split(digestSpec, ":")
	if len(tokens) != 2 {
		return "", fmt.Errorf("invalid incoming digest specification %s", digestSpec)
	}
	if tokens[0] != sha256DigestName {
		return "", fmt.Errorf("unsupported digest type %s", digestSpec)
	}

	bytes, err := hex.DecodeString(tokens[1])
	if err != nil {
		return "", fmt.Errorf("invalid digest hex %s", tokens[1])
	}

	return base64.StdEncoding.EncodeToString(bytes), nil
}

func computeKey(digest string) string {
	return fmt.Sprintf("blobs/%s", digest)
}
