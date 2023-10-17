package badger

import (
	"badgerlit/sdk"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

var (
	_ sdk.Storage = new(DB)
)

type DB struct {
	db *badger.DB

	keyDiscardTask *KeyDiscardTask

	logger badger.Logger

	mutex    sync.Mutex
	running  bool
	disposed bool
}

func New(config *sdk.Config) *DB {
	// logger
	logger := newLogger(int(badger.INFO))
	{
		logger.SetPrefix(__LOGGER_PREFIX)

		flags, err := config.LogFlags()
		logger.SetFlags(flags)

		if err != nil {
			logger.Errorf("%v", err)
		}
	}

	// badger.Options
	opts := badger.DefaultOptions(config.DataPath).
		WithLogger(logger)

	// badger.DB
	badgerDB, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}

	keyDiscardTask := &KeyDiscardTask{
		BadgerDB:           badgerDB,
		KeyDiscardInterval: config.KeyDiscardInterval,
		KeyDiscardRatio:    config.KeyDiscardRatio,
		Logger:             logger,
	}
	keyDiscardTask.init()

	return &DB{
		db:             badgerDB,
		keyDiscardTask: keyDiscardTask,
		logger:         logger,
	}
}

// Start implements sdk.Storage.
func (db *DB) Start(ctx context.Context) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.disposed {
		panic(fmt.Sprintf("%T had been disposed", db))
	}
	if db.running {
		return
	}

	db.logger.Infof("Ready")

	db.keyDiscardTask.run()
	db.running = true
}

// Stop implements sdk.Storage.
func (db *DB) Stop(ctx context.Context) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if !db.disposed {
		db.logger.Infof("Stopping")

		db.disposed = true
		db.running = false
		db.keyDiscardTask.stop()

		db.logger.Infof("Stopped")
	}
}

// Del implements sdk.Storage.
func (db *DB) Del(key []byte) error {
	if !db.running {
		return sdk.ErrDatabaseUnavailable
	}

	err := db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
	return err
}

// Exists implements sdk.Storage.
func (db *DB) Exists(key []byte) (bool, error) {
	if !db.running {
		return false, sdk.ErrDatabaseUnavailable
	}

	err := db.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Expire implements sdk.Storage.
func (db *DB) Expire(key []byte, lease time.Duration) (bool, error) {
	if !db.running {
		return false, sdk.ErrDatabaseUnavailable
	}

	err := db.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			entry := badger.NewEntry(item.Key(), val).
				WithDiscard().
				WithTTL(lease)

			return txn.SetEntry(entry)
		})
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Get implements sdk.Storage.
func (db *DB) Get(key []byte) ([]byte, error) {
	if !db.running {
		return nil, sdk.ErrDatabaseUnavailable
	}

	var reply []byte

	err := db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			reply = val
			return nil
		})
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, sdk.ErrNil
		}
		return nil, err
	}
	return reply, nil
}

// IncrBy implements sdk.Storage.
func (db *DB) IncrBy(key []byte, increment int64, constraints ...sdk.Constraint[int64]) (int64, error) {
	if !db.running {
		return 0, sdk.ErrDatabaseUnavailable
	}

	var result int64 = 0

	err := db.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}

		var value []byte
		{
			var number int64 = 0

			if item != nil {
				err = item.Value(func(val []byte) error {
					n, err := strconv.ParseInt(string(val), 10, 64)
					if err != nil {
						return sdk.ErrNonInteger
					}

					number = n
					return nil
				})
				if err != nil {
					return err
				}
			}

			// add increment & export
			result = number + increment

			// check
			for _, constraint := range constraints {
				ok := constraint.Check(result)
				if !ok {
					return sdk.ErrViolateConstraints
				}
			}

			// export
			value = []byte(strconv.FormatInt(result, 10))
		}

		entry := badger.NewEntry(key, value).WithDiscard()
		return txn.SetEntry(entry)
	})
	return result, err
}

// IncrByFloat implements sdk.Storage.
func (db *DB) IncrByFloat(key []byte, increment float64, constraints ...sdk.Constraint[float64]) (float64, error) {
	if !db.running {
		return 0, sdk.ErrDatabaseUnavailable
	}

	var result float64 = 0

	err := db.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}

		var value []byte
		{
			var number float64 = 0

			if item != nil {
				err = item.Value(func(val []byte) error {
					n, err := strconv.ParseFloat(string(val), 64)
					if err != nil {
						return sdk.ErrNonInteger
					}

					number = n
					return nil
				})
				if err != nil {
					return err
				}
			}

			// add increment & export
			result = number + increment

			// check
			for _, constraint := range constraints {
				ok := constraint.Check(result)
				if !ok {
					return sdk.ErrViolateConstraints
				}
			}

			// export
			value = []byte(strconv.FormatFloat(result, 'f', 4, 64))
		}

		entry := badger.NewEntry(key, value).WithDiscard()
		return txn.SetEntry(entry)
	})
	return result, err
}

// Persist implements sdk.Storage.
func (db *DB) Persist(key []byte) (bool, error) {
	if !db.running {
		return false, sdk.ErrDatabaseUnavailable
	}

	err := db.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		return item.Value(func(val []byte) error {
			entry := badger.NewEntry(item.Key(), val).
				WithDiscard()

			return txn.SetEntry(entry)
		})
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Scan implements sdk.Storage.
func (db *DB) Scan(cursor []byte, opts sdk.ScanOptions) ([][]byte, error) {
	if !db.running {
		return nil, sdk.ErrDatabaseUnavailable
	}

	var kvs [][]byte

	err := db.db.View(func(txn *badger.Txn) error {
		iterOpts := badger.DefaultIteratorOptions
		ScanOptionsWrapper(opts).apply(&iterOpts)

		iter := txn.NewIterator(iterOpts)
		defer iter.Close()

		if len(cursor) == 0 {
			iter.Rewind()
		} else {
			iter.Seek(cursor)
		}

		for ; iter.Valid(); iter.Next() {
			item := iter.Item()

			if iterOpts.PrefetchValues {
				err := item.Value(func(val []byte) error {
					kvs = append(kvs, item.Key(), val)
					return nil
				})
				if err != nil {
					return err
				}
			} else {
				kvs = append(kvs, item.Key())
			}
		}
		return nil
	})
	return kvs, err
}

// Set implements sdk.Storage.
func (db *DB) Set(key []byte, value []byte) error {
	if !db.running {
		return sdk.ErrDatabaseUnavailable
	}

	err := db.db.Update(func(txn *badger.Txn) error {
		entry := badger.NewEntry(key, value).
			WithDiscard()

		return txn.SetEntry(entry)
	})
	return err
}

// Ttl implements sdk.Storage.
func (db *DB) Ttl(key []byte) (ok bool, ttl int64, err error) {
	if !db.running {
		return false, 0, sdk.ErrDatabaseUnavailable
	}

	var expireAt uint64

	err = db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		expireAt = item.ExpiresAt()
		return nil
	})

	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return false, sdk.NONE_TTL, nil
		}
		return false, sdk.NONE_TTL, err
	}

	if expireAt == 0 {
		return true, sdk.UNSET_LEASE, nil
	}

	var now uint64 = uint64(time.Now().Unix())
	if now > expireAt {
		return false, sdk.NONE_TTL, nil
	}
	return true, int64(expireAt - now), nil
}
