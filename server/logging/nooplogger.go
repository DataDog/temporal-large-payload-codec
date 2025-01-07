// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

package logging

// NoopLogger is a logger that emits no logs
type NoopLogger struct {
}

// NewNoopLogger creates a new logger that emits no logs.
func NewNoopLogger() *NoopLogger {
	return &NoopLogger{}
}

func (l *NoopLogger) Debug(_ string, _ ...interface{}) {
}

func (l *NoopLogger) Info(_ string, _ ...interface{}) {
}

func (l *NoopLogger) Error(_ string, _ ...interface{}) {
}
