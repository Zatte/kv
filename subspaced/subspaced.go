package subspaced

import (
	"context"

	"github.com/zatte/fdbtuple"
	"github.com/zatte/fdbtuple/subspace"
	"github.com/zatte/kv"
)

type SubSpacedDb struct {
	kv.OrderedTransactional
	subspace subspace.Subspace
}

type SubSpacedDbTransaction struct {
	kv.OrderedTransaction
	subspace subspace.Subspace
}

type SubSpacedDbIterator struct {
	kv.Iterator
	subspace subspace.Subspace
}

// New creates a DB which where all keys are located under the prefix. Only the exact
// set of prefixes will be accessable for this DB.
func New(db kv.OrderedTransactional, prefixes ...interface{}) *SubSpacedDb {
	t := fdbtuple.Tuple{}
	//ss := subspace.Sub()
	for _, p := range prefixes {
		t = append(t, p)
		//ss = ss.Sub(p)
	}
	return &SubSpacedDb{db, subspace.Sub(t)}
}

// NewFromStr creates a DB which where all keys are located under the prefix. Only the exact
// set of prefixes will be accessable for this DB. Supports all connections strings of
// of kv.New()
func NewFromStr(connectionString string, prefixes ...interface{}) (*SubSpacedDb, error) {
	db, err := kv.New(connectionString)
	if err != nil {
		return nil, err
	}

	return New(db, prefixes...), nil
}

// Get gets the value of a key within a single query transaction
func (bs *SubSpacedDb) Get(ctx context.Context, key []byte) (res []byte, err error) {
	res, err = bs.OrderedTransactional.Get(ctx, bs.subspace.Pack(fdbtuple.Tuple{key}))
	return res, err
}

// Put sets the value of a key within a single query transaction
func (bs *SubSpacedDb) Put(ctx context.Context, key, value []byte) error {
	err := bs.OrderedTransactional.Put(ctx, bs.subspace.Pack(fdbtuple.Tuple{key}), value)
	return err
}

// Delete removes a key within a single transaction
func (bs *SubSpacedDb) Delete(ctx context.Context, key []byte) error {
	err := bs.OrderedTransactional.Delete(ctx, bs.subspace.Pack(fdbtuple.Tuple{key}))
	return err
}

// NewTransaction for batching multiple values inside a transaction
func (bs *SubSpacedDb) NewTransaction(ctx context.Context, readOnly bool) (kv.OrderedTransaction, error) {
	ot, err := bs.OrderedTransactional.NewTransaction(ctx, readOnly)
	return &SubSpacedDbTransaction{ot, bs.subspace}, err
}

func (bst *SubSpacedDbTransaction) Seek(ctx context.Context, StartKey []byte) (kv.Iterator, error) {
	it, err := bst.OrderedTransaction.Seek(ctx, bst.subspace.Pack(fdbtuple.Tuple{StartKey}))
	return &SubSpacedDbIterator{it, bst.subspace}, err
}

// Get gets the value of a key within a single query transaction
func (bs *SubSpacedDbTransaction) Get(ctx context.Context, key []byte) (res []byte, err error) {
	res, err = bs.OrderedTransaction.Get(ctx, bs.subspace.Pack(fdbtuple.Tuple{key}))
	return res, err
}

// Put sets the value of a key within a single query transaction
func (bs *SubSpacedDbTransaction) Put(ctx context.Context, key, value []byte) error {
	err := bs.OrderedTransaction.Put(ctx, bs.subspace.Pack(fdbtuple.Tuple{key}), value)
	return err
}

// Delete removes a key within a single transaction
func (bs *SubSpacedDbTransaction) Delete(ctx context.Context, key []byte) error {
	err := bs.OrderedTransaction.Delete(ctx, bs.subspace.Pack(fdbtuple.Tuple{key}))
	return err
}

func (it *SubSpacedDbIterator) Next(ctx context.Context) (key, value []byte, err error) {
	k, v, e := it.Iterator.Next(ctx)

	if e != nil {
		return nil, nil, e
	}

	t, err := it.subspace.Unpack(fdbtuple.Key(k))
	if err != nil {
		return nil, nil, err
	}

	return t[0].([]byte), v, nil
}
