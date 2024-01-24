package logger

import (
	"io"
	"log/slog"
	"os"
)

// Logger is an interface for logging
type Logger interface {
	Info(msg string, args ...interface{})
	Error(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Fatal(msg string, args ...interface{})
}

// log is a wrapper around slog.Logger
type log struct {
	logger *slog.Logger
}

// New creates a new logger
func New(out io.Writer) Logger {
	jsonHandler := slog.NewJSONHandler(out, nil)
	baseLogger := slog.New(jsonHandler)
	slog.SetDefault(baseLogger)

	return &log{
		logger: baseLogger,
	}
}

// Info logs a message with info level
func (l *log) Info(msg string, args ...interface{}) {
	l.logger.Info(msg, args...)
}

// Warn logs a message with warn level
func (l *log) Warn(msg string, args ...interface{}) {
	l.logger.Warn(msg, args...)
}

// Error logs a message with error level
func (l *log) Error(msg string, args ...interface{}) {
	l.logger.Error(msg, args...)
}

// Fatal logs a message with fatal level
func (l *log) Fatal(msg string, args ...interface{}) {
	l.logger.Error(msg, args...)
	os.Exit(1)
}

// Default creates a new logger with os.Stdout
func Default() Logger {
	return New(os.Stdout)
}
