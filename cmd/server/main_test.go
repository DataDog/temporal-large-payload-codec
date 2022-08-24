package main

import (
	"github.com/DataDog/temporal-large-payload-codec/server/storage"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/memory"
	"github.com/DataDog/temporal-large-payload-codec/server/storage/s3"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateDriver(t *testing.T) {
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
			description:    "s3 driver",
			testEnv:        map[string]string{"AWS_REGION": "eu-central-1", "BUCKET": "my-bucket"},
			driverName:     "s3",
			expectedDriver: &s3.Driver{},
			expectError:    false,
		},
	} {
		t.Run(scenario.description, func(t *testing.T) {
			envCleaner := envSetter(scenario.testEnv)
			t.Cleanup(envCleaner)

			driver, err := createDriver(scenario.driverName)
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
