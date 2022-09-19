package v2

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_computeKey(t *testing.T) {
	h := blobHandler{}

	testCase := []struct {
		name        string
		namespace   string
		digest      string
		meta        map[string][]byte
		expectedKey string
		expectError bool
	}{
		{
			name:        "no prefix",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{},
			expectedKey: "/blobs/foo/common/sha256:1234/sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			expectError: false,
		},
		{
			name:        "valid prefix",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{keyPrefixName: []byte("a/b/c")},
			expectedKey: "/blobs/foo/custom/a/b/c/sha256:1234/sha256:02b711154c4e88a46ff26dc96f492ce38c8c9fe00f3b6b2ea1ef6c209a2f3bd7",
			expectError: false,
		},
		{
			name:        "invalid prefix",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{keyPrefixName: []byte("../../a")},
			expectedKey: "",
			expectError: true,
		},
		{
			name:        "invalid prefix ii",
			namespace:   "foo",
			digest:      "sha256:1234",
			meta:        map[string][]byte{keyPrefixName: []byte("a$(foo)b")},
			expectedKey: "",
			expectError: true,
		},
	}

	for _, scenario := range testCase {
		t.Run(scenario.name, func(t *testing.T) {
			key, err := h.computeKey(scenario.namespace, scenario.digest, scenario.meta)
			if scenario.expectError {
				assert.Error(t, err)
				assert.Empty(t, key)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, scenario.expectedKey, key)
			}
		})
	}
}
