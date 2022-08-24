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
