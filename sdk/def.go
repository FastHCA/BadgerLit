package sdk

import (
	"context"
	"log"
	"time"
)

const (
	ErrNonInteger          = Error("value is not an integer or out of range")
	ErrDatabaseUnavailable = Error("database is unavailable")
	ErrNil                 = Error("nil")
	ErrViolateConstraints  = Error("violate constraints")

	UNSET_LEASE = -1
	NONE_TTL    = 0

	DefaultListenAddress      = ":8962"
	DefaultEngine             = "file"
	DefaultDataPath           = "./.data/dump"
	DefaultKeyDiscardInterval = 90 * time.Second
	DefaultKeyDiscardRatio    = 0.7
	DefaultLogFlags           = log.Lmsgprefix | log.LstdFlags

	LOG_FLAG_TOKEN_DATE      = "date"
	LOG_FLAG_TOKEN_TIME      = "time"
	LOG_FLAG_TOKEN_UTC       = "utc"
	LOG_FLAG_TOKEN_MSGPREFIX = "msgprefix"
	LOG_FLAG_TOKEN_DEFAULT   = "default"
	LOG_FLAG_TOKEN_NONE      = "none"
)

type (
	Storage interface {
		Start(ctx context.Context)
		Stop(ctx context.Context)

		Get(key []byte) ([]byte, error)
		Set(key []byte, value []byte) error
		IncrBy(key []byte, increment int64, constraints ...Constraint[int64]) (int64, error)
		IncrByFloat(key []byte, increment float64, constraints ...Constraint[float64]) (float64, error)

		Scan(cursor []byte, opts ScanOptions) ([][]byte, error)
		Ttl(key []byte) (ok bool, ttl int64, err error)
		Exists(key []byte) (bool, error)
		Del(key []byte) error
		Expire(key []byte, lease time.Duration) (bool, error)
		Persist(key []byte) (bool, error)
	}

	ScanOption interface {
		apply(opts *ScanOptions)
	}

	ScanOptions struct {
		PrefetchValues bool
		PrefetchSize   int
		Prefix         []byte
		Reverse        bool
	}

	Constraint[T comparable] interface {
		Check(v T) bool
	}
)
