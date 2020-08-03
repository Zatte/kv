package kv

import (
	"context"
	"net/url"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

const DataStoreKind = "keyvalue"

type datastoreKeyValue struct {
	Key *datastore.Key `datastore:"__key__"`
	Val []byte         `datastore:"val,noindex"`
}

type DatastoreDB struct {
	*datastore.Client
	cancel func()
}

type datastoreTransaction struct {
	*datastore.Transaction
	*datastore.Client
}

type datastoreIterator struct {
	*datastore.Iterator
}

func NewDatastoreDbFromUrl(u *url.URL) (*DatastoreDB, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create a datastore client. In a typical application, you would create
	// a single client which is reused for every datastore operation.
	dsClient, err := datastore.NewClient(ctx, u.Host)
	if err != nil {
		cancel()
		return nil, err
	}

	return &DatastoreDB{dsClient, cancel}, nil
}

// datastore db

// Get gets the value of a key within a single query transaction
func (dsDb *DatastoreDB) Close() error {
	defer dsDb.cancel()
	return dsDb.Client.Close()
}

// Get gets the value of a key within a single query transaction
func (dsDb *DatastoreDB) Get(ctx context.Context, key []byte) (res []byte, err error) {
	k := datastore.NameKey(DataStoreKind, string(key), nil)
	e := &datastoreKeyValue{}
	if err := dsDb.Client.Get(ctx, k, e); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = ErrNotFound
		}
		return nil, err
	}
	return []byte(e.Val), err
}

// Put sets the value of a key within a single query transaction
func (dsDb *DatastoreDB) Put(ctx context.Context, key, value []byte) error {
	k := datastore.NameKey(DataStoreKind, string(key), nil)
	e := &datastoreKeyValue{
		Key: k,
		Val: value,
	}
	if _, err := dsDb.Client.Put(ctx, k, e); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = ErrNotFound
		}
		return err
	}
	return nil
}

// Delete removes a key within a single transaction
func (dsDb *DatastoreDB) Delete(ctx context.Context, key []byte) error {
	k := datastore.NameKey(DataStoreKind, string(key), nil)
	if err := dsDb.Client.Delete(ctx, k); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = ErrNotFound
		}
		return err
	}
	return nil
}

// NewTransaction for batching multiple values inside a transaction
func (dsDb *DatastoreDB) NewTransaction(ctx context.Context, readOnly bool) (OrderedTransaction, error) {
	tx, err := dsDb.Client.NewTransaction(ctx)
	if err != nil {
		return nil, err
	}

	return &datastoreTransaction{
		tx,
		dsDb.Client, // save for iterators later on
	}, nil
}

// datastoreTransaction

// Seeks initializes an iterator at the given key (inclusive)

func (dsDb *datastoreTransaction) Close() error {
	return dsDb.Discard(context.Background())
}

// Get gets the value of a key within a single query transaction
func (dsDb *datastoreTransaction) Get(ctx context.Context, key []byte) (res []byte, err error) {
	k := datastore.NameKey(DataStoreKind, string(key), nil)
	e := &datastoreKeyValue{}
	if err := dsDb.Transaction.Get(k, e); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = ErrNotFound
		}
		return nil, err
	}
	return []byte(e.Val), err
}

// Put sets the value of a key within a single query transaction
func (dsDb *datastoreTransaction) Put(ctx context.Context, key, value []byte) error {
	k := datastore.NameKey(DataStoreKind, string(key), nil)
	e := &datastoreKeyValue{
		Key: k,
		Val: value,
	}
	if _, err := dsDb.Transaction.Put(k, e); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = ErrNotFound
		}
		return err
	}
	return nil
}

// Delete removes a key within a single transaction
func (dsDb *datastoreTransaction) Delete(ctx context.Context, key []byte) error {
	k := datastore.NameKey(DataStoreKind, string(key), nil)
	if err := dsDb.Transaction.Delete(k); err != nil {
		if err == datastore.ErrNoSuchEntity {
			err = ErrNotFound
		}
		return err
	}
	return nil
}

func (dsDb *datastoreTransaction) Seek(ctx context.Context, StartKey []byte) (Iterator, error) {
	k := datastore.NameKey(DataStoreKind, string(StartKey), nil)
	query := datastore.NewQuery(DataStoreKind).
		Filter("__key__ >=", k).
		Order("__key__") //.Transaction(dsDb.Transaction)
	it := dsDb.Client.Run(ctx, query)
	return &datastoreIterator{
		it,
	}, nil
}

// Discard removes all sides effects of the transaction
func (dsDb *datastoreTransaction) Discard(ctx context.Context) error {
	return dsDb.Transaction.Rollback()
}

// Commit persists all side effects of the transaction and returns an error if there are any conflics
func (dsDb *datastoreTransaction) Commit(ctx context.Context) error {
	_, err := dsDb.Transaction.Commit()
	return err
}

// datastoreIterator

// Next yeilds the next key-value in iterator. Key-values can not be re-used between iterations. Make sure top copy the values if you must.
func (it *datastoreIterator) Next(ctx context.Context) (key, value []byte, err error) {
	kv := &datastoreKeyValue{}
	_, err = it.Iterator.Next(kv)
	if err == iterator.Done {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	return []byte(kv.Key.String()), kv.Val, nil
}

// Close must always be called to clean up iterators.
func (dsDb *datastoreIterator) Close() error {
	return nil
}
