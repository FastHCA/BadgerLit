package badger

import (
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type KeyDiscardTask struct {
	BadgerDB *badger.DB

	KeyDiscardInterval time.Duration
	KeyDiscardRatio    float64

	Logger badger.Logger

	mutex       sync.Mutex
	done        chan struct{}
	initialized bool
	disposed    bool
}

func (task *KeyDiscardTask) init() {
	task.mutex.Lock()
	defer task.mutex.Unlock()

	if task.initialized {
		return
	}

	task.done = make(chan struct{})
	task.initialized = true
}

func (task *KeyDiscardTask) run() {
	if !task.initialized {
		panic(fmt.Sprintf("%T don't be initialized yet", task))
	}

	ticker := time.NewTicker(task.KeyDiscardInterval)

	go func() {
		defer ticker.Stop()

		for {
			select {
			case <-task.done:
				return
			case <-ticker.C:
				err := task.BadgerDB.RunValueLogGC(task.KeyDiscardRatio)
				if err != nil {
					task.Logger.Infof("RunValueLogGC()")
				}
			}
		}
	}()
}

func (task *KeyDiscardTask) stop() {
	task.mutex.Lock()
	defer task.mutex.Unlock()

	if !task.disposed {
		task.disposed = true
		close(task.done)
	}
}
