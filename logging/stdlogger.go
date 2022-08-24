package logging

import (
	"log"
	"os"
)

// BuiltinLogger is a logger using Go's built-in logger.
type BuiltinLogger struct {
	logger *log.Logger
}

// NewBuiltinLogger creates a new logger using Go's built-in logger.
func NewBuiltinLogger() *BuiltinLogger {
	return &BuiltinLogger{logger: log.New(os.Stdout, "", log.Ldate|log.Ltime)}
}

func (l *BuiltinLogger) Debug(msg string, keyvals ...interface{}) {
	logLine := append([]interface{}{"debug:", msg}, keyvals...)
	l.log(append(logLine))
}

func (l *BuiltinLogger) Info(msg string, keyvals ...interface{}) {
	logLine := append([]interface{}{"info:", msg}, keyvals...)
	l.log(append(logLine))
}

func (l *BuiltinLogger) Error(msg string, keyvals ...interface{}) {
	logLine := append([]interface{}{"error:", msg}, keyvals...)
	l.log(append(logLine))
}

func (l *BuiltinLogger) log(logLine []interface{}) {
	l.logger.Printf("%v", logLine)
}
