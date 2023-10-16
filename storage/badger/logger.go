package badger

import (
	"log"
	"os"

	"github.com/dgraph-io/badger/v4"
)

var (
	_ badger.Logger = new(Logger)
)

type Logger struct {
	*log.Logger
	level int
}

func newLogger(level int) *Logger {
	return &Logger{Logger: log.New(os.Stderr, "badger ", log.LstdFlags), level: level}
}

// Debugf implements badger.Logger.
func (l *Logger) Debugf(f string, v ...interface{}) {
	if l.level <= int(badger.DEBUG) {
		l.Printf("DEBUG: "+f, v...)
	}
}

// Errorf implements badger.Logger.
func (l *Logger) Errorf(f string, v ...interface{}) {
	if l.level <= int(badger.ERROR) {
		l.Printf("ERROR: "+f, v...)
	}
}

// Infof implements badger.Logger.
func (l *Logger) Infof(f string, v ...interface{}) {
	if l.level <= int(badger.INFO) {
		l.Printf("INFO: "+f, v...)
	}
}

// Warningf implements badger.Logger.
func (l *Logger) Warningf(f string, v ...interface{}) {
	if l.level <= int(badger.WARNING) {
		l.Printf("WARNING: "+f, v...)
	}
}
