// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.

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
