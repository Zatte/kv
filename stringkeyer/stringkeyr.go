package stringkeyer

import (
	"context"

	"github.com/zatte/kv"
)

type StringKeyerDb struct {
	kv.OrderedTransactional
}

type StringKeyerDbTransaction struct {
	kv.OrderedTransaction
}

type StringKeyerDbIterator struct {
	kv.Iterator
}

// New creates a new db where keys are strings and not bytes. Only a wrapper for []byte(key string)
func New(db kv.OrderedTransactional) *StringKeyerDb {
	return &StringKeyerDb{db}
}

// Get gets the value of a key within a single query transaction
func (bs *StringKeyerDb) Get(ctx context.Context, key string) (res []byte, err error) {
	return bs.OrderedTransactional.Get(ctx, []byte(key))
}

// Put sets the value of a key within a single query transaction
func (bs *StringKeyerDb) Put(ctx context.Context, key string, value []byte) error {
	return bs.OrderedTransactional.Put(ctx, []byte(key), value)
}

// Delete removes a key within a single transaction
func (bs *StringKeyerDb) Delete(ctx context.Context, key string) error {
	return bs.OrderedTransactional.Delete(ctx, []byte(key))
}

// NewTransaction for batching multiple values inside a transaction
func (bs *StringKeyerDb) NewTransaction(ctx context.Context, readOnly bool) (kv.OrderedTransaction, error) {
	return bs.OrderedTransactional.NewTransaction(ctx, readOnly)
}

func (bst *StringKeyerDbTransaction) Seek(ctx context.Context, StartKey string) (kv.Iterator, error) {
	return bst.OrderedTransaction.Seek(ctx, []byte(StartKey))
}

// Get gets the value of a key within a single query transaction
func (bs *StringKeyerDbTransaction) Get(ctx context.Context, key string) (res []byte, err error) {
	return bs.OrderedTransaction.Get(ctx, []byte(key))
}

// Put sets the value of a key within a single query transaction
func (bs *StringKeyerDbTransaction) Put(ctx context.Context, key string, value []byte) error {
	err := bs.OrderedTransaction.Put(ctx, []byte(key), value)
	return err
}

// Delete removes a key within a single transaction
func (bs *StringKeyerDbTransaction) Delete(ctx context.Context, key string) error {
	err := bs.OrderedTransaction.Delete(ctx, []byte(key))
	return err
}

func (it *StringKeyerDbIterator) Next(ctx context.Context) (key string, value []byte, err error) {
	k, v, e := it.Iterator.Next(ctx)
	return string(k), v, e
}
