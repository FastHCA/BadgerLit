package badger

import (
	"badgerlit/sdk"

	"github.com/dgraph-io/badger/v4"
)

type ScanOptionsWrapper sdk.ScanOptions

func (opts ScanOptionsWrapper) apply(iterOpts *badger.IteratorOptions) {
	iterOpts.PrefetchValues = opts.PrefetchValues
	if opts.PrefetchSize > 0 {
		iterOpts.PrefetchSize = opts.PrefetchSize
	}
	iterOpts.Prefix = opts.Prefix
	iterOpts.Reverse = opts.Reverse
}
