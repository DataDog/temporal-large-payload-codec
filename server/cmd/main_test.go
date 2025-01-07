// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package main

import (
	"context"
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/gcs"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/s3"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	dummyGCSCredentials = `
{
  "client_id": "foo.apps.googleusercontent.com",
  "client_secret": "snafu",
  "refresh_token": "token",
  "type": "authorized_user"
}
`
)

func TestCreateDriver(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "lps-test")
	if err != nil {
		require.NoError(t, err)
	}

	// write a dummy credentials GCS file
	_, err = tmpFile.WriteString(dummyGCSCredentials)
	if err != nil {
		require.NoError(t, err)
	}
	err = tmpFile.Sync()
	if err != nil {
		require.NoError(t, err)
	}

	defer func() {
		_ = os.Remove(tmpFile.Name())
	}()

	type testCases struct {
		description    string
		testEnv        map[string]string
		driverName     string
		expectedDriver storage.Driver
		expectError    bool
	}

	for _, scenario := range []testCases{
		{
			description:    "empty driver name",
			testEnv:        map[string]string{},
			driverName:     "",
			expectedDriver: nil,
			expectError:    true,
		},
		{
			description:    "unknown driver name",
			testEnv:        map[string]string{},
			driverName:     "snafu",
			expectedDriver: nil,
			expectError:    true,
		},
		{
			description:    "memory driver",
			testEnv:        map[string]string{},
			driverName:     "memory",
			expectedDriver: &memory.Driver{},
			expectError:    false,
		},
		{
			description:    "MEMORY driver",
			testEnv:        map[string]string{},
			driverName:     "MEMORY",
			expectedDriver: &memory.Driver{},
			expectError:    false,
		},
		{
			description: "s3 driver",
			testEnv: map[string]string{
				"AWS_REGION": "eu-central-1",
				"BUCKET":     "my-bucket",
			},
			driverName:     "s3",
			expectedDriver: &s3.Driver{},
			expectError:    false,
		},
		{
			description: "gcs driver",
			testEnv: map[string]string{
				"BUCKET":                         "my-bucket",
				"GOOGLE_APPLICATION_CREDENTIALS": tmpFile.Name(),
			},
			driverName:     "gcs",
			expectedDriver: &gcs.Driver{},
			expectError:    false,
		},
	} {
		t.Run(scenario.description, func(t *testing.T) {
			ctx := context.Background()
			envCleaner := envSetter(scenario.testEnv)
			t.Cleanup(envCleaner)

			driver, err := createDriver(ctx, scenario.driverName)
			if scenario.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.IsType(t, scenario.expectedDriver, driver)
		})
	}
}

func envSetter(envs map[string]string) (closer func()) {
	originalEnvs := map[string]string{}

	for name, value := range envs {
		if originalValue, ok := os.LookupEnv(name); ok {
			originalEnvs[name] = originalValue
		}
		_ = os.Setenv(name, value)
	}

	return func() {
		for name := range envs {
			origValue, has := originalEnvs[name]
			if has {
				_ = os.Setenv(name, origValue)
			} else {
				_ = os.Unsetenv(name)
			}
		}
	}
}
