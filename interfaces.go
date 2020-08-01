package kv

import "context"

// Basic is the simplest version of a key/value store
type Basic interface {
	Get(ctx context.Context, Key []byte) ([]byte, error)
	Put(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

// BasicTransactional bundles all basic operations into an atomic operation.
// it is safe to call Discard or Commit multiples times; only the first call
// should be respected. BasicTransactional implementations is expected to be
// returned by a NewTransaction(readOnly bool) method of a KeyValue store
type BasicTransaction interface {
	Basic
	Discard(ctx context.Context) error
	Commit(ctx context.Context) error
}

type BasicTransactional interface {
	Basic
	NewTransaction(ctx context.Context, ReadOnly bool) (BasicTransaction, error)
}

// Iterator scans a key space in a memory bound fashion.
// follow google design guidelines https://github.com/googleapis/google-cloud-go/wiki/Iterator-Guidelines
type Iterator interface {
	Next(ctx context.Context) (key, value []byte, err error)
	Close() error
}

// Ordered is an extention to the basic store by also providing scan methods.
// all scans must be byte-wise lexicographical sorting order.
type Ordered interface {
	Basic
	Seek(ctx context.Context, StartKey []byte) (Iterator, error)
}

// OrderedTransaction is an extention to the basic store by also providing scan methods.
// all scans must be byte-wise lexicographical sorting order.
// OrderedTransactional implementations is expected to be
// returned by a NewTransaction(readOnly bool) method of a KeyValue store
type OrderedTransaction interface {
	Ordered
	Discard(ctx context.Context) error
	Commit(ctx context.Context) error
}

type OrderedTransactional interface {
	Basic
	NewTransaction(ctx context.Context, ReadOnly bool) (OrderedTransaction, error)
}
