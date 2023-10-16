package badger_test

import (
	"badgerlit/sdk"
	"badgerlit/storage/badger"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDB(t *testing.T) {
	config := sdk.Config{
		Engine:             "file",
		DataPath:           "./.data/dump",
		KeyDiscardInterval: 5 * time.Second,
		KeyDiscardRatio:    0.7,
		LogFlagsToken:      strings.Split("default,msgprefix", ","),
	}

	db := badger.New(&config)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	go func() {
		db.Start(ctx)
		err := db.Set([]byte("foo"), []byte("FOO"))
		if err != nil {
			fmt.Printf("%+v\n", err)
		}
		value, err := db.Get([]byte("foo"))
		if err != nil {
			fmt.Printf("%+v\n", err)
		}
		fmt.Printf("%+v\n", string(value))
	}()

	select {
	case <-ctx.Done():
		db.Stop(context.Background())
	}
}
