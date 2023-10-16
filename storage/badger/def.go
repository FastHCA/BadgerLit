package badger

import (
	"badgerlit/sdk"
	"log"
)

const (
	__LOGGER_PREFIX = "[badger] "
	__LOGGER_FLAGS  = sdk.DefaultLogFlags
)

var (
	defaultLogger = log.New(log.Writer(), __LOGGER_PREFIX, __LOGGER_FLAGS)
)
