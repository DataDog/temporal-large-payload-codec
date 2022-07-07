package s3

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestAwsDigest(t *testing.T) {
	const expectedAwsDigest = "cXrFBpUNoMy2QEzdXnWR9yAYogy8onyKQj6cnlYmrGE="

	data := []byte("This is a test string")

	// NOTE: this is directly pulled from codec.go; consider extracting into utility package
	sha2 := sha256.New()
	sha2.Write(data)
	inDigestSpec := "sha256:" + hex.EncodeToString(sha2.Sum(nil))

	awsDigest, err := computeAwsDigest(inDigestSpec)
	if err != nil {
		t.Fatalf("failed to compute digest for %v (%s): %v", data, inDigestSpec, err)
	}

	if awsDigest != expectedAwsDigest {
		t.Fatalf("expected digest %s got %s", expectedAwsDigest, awsDigest)
	}
}
