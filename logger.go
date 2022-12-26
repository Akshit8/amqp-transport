package amqptransport

import (
	"fmt"
	"log"
)

type Logger interface {
	Info(formtat string, args ...interface{})
	Error(formtat string, args ...interface{})
	Debug(formtat string, args ...interface{})
	Fatal(formtat string, args ...interface{})
}

type prefix string

const loggingPrefix = prefix("[amqptransport]")

type DefaultLogger struct{}

func NewDefaultLogger() Logger {
	return DefaultLogger{}
}

func (DefaultLogger) Info(format string, args ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO %s", loggingPrefix, format), args...)
}

func (DefaultLogger) Error(format string, args ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR %s", loggingPrefix, format), args...)
}

func (DefaultLogger) Debug(format string, args ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG %s", loggingPrefix, format), args...)
}

func (DefaultLogger) Fatal(format string, args ...interface{}) {
	log.Fatalf(fmt.Sprintf("%s FATAL %s", loggingPrefix, format), args...)
}
