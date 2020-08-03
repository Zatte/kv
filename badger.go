package kv

import (
	"context"
	"net/url"

	"github.com/dgraph-io/badger/v2"
)

type BadgerDB struct {
	*badger.DB
}

type badgerTransaction struct {
	*badger.Txn
}

type badgerIterator struct {
	*badger.Iterator
}

func NewBadgerDbFromUrl(u *url.URL) (*BadgerDB, error) {
	var db *badger.DB
	var err error
	passw, _ := u.User.Password()
	passw = passw + "12345678901234567890123456789012" // make sure the password is at least 32 chars by appending a default suffix

	if err != nil {
		return nil, err
	}

	if u.Query().Get("memory") == "true" {
		db, err = badger.Open(badger.DefaultOptions("").WithInMemory(true))
	} else {
		db, err = badger.Open(
			badger.DefaultOptions(u.Path).
				// Lower RAM usage without mmap
				WithNumVersionsToKeep(0).
				WithEncryptionKey([]byte(passw)[:32]).
				WithTruncate(true), // this would trucate faulty value logs; something that should NOT be problematic with syncWrites(true)
		)
	}

	if err != nil {
		return nil, err
	}

	return NewbadgerFromDB(db)
}

func NewbadgerFromDB(db *badger.DB) (*BadgerDB, error) {
	return &BadgerDB{
		db,
	}, nil
}

// badger db

// Get gets the value of a key within a single query transaction
func (bdb *BadgerDB) Close() error {
	return bdb.DB.Close()
}

// Get gets the value of a key within a single query transaction
func (bdb *BadgerDB) Get(ctx context.Context, key []byte) (res []byte, err error) {
	err = bdb.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		res, err = item.ValueCopy(res)
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = ErrNotFound
	}
	return res, err
}

// Put sets the value of a key within a single query transaction
func (bdb *BadgerDB) Put(ctx context.Context, key, value []byte) error {
	err := bdb.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
	if err == badger.ErrKeyNotFound {
		err = ErrNotFound
	}
	return err
}

// Delete removes a key within a single transaction
func (bdb *BadgerDB) Delete(ctx context.Context, key []byte) error {
	return bdb.DB.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err == badger.ErrKeyNotFound {
			return ErrNotFound
		}
		return err
	})
}

// NewTransaction for batching multiple values inside a transaction
func (bdb *BadgerDB) NewTransaction(ctx context.Context, readOnly bool) (OrderedTransaction, error) {
	return &badgerTransaction{
		bdb.DB.NewTransaction(!readOnly),
	}, nil
}

// badgerTransaction

// Seeks initializes an iterator at the given key (inclusive)

func (bdb *badgerTransaction) Close() error {
	return bdb.Discard(context.Background())
}

// Get gets the value of a key within a single query transaction
func (bdb *badgerTransaction) Get(ctx context.Context, key []byte) (res []byte, err error) {
	item, err := bdb.Txn.Get(key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			err = ErrNotFound
		}
		return res, err
	}
	return item.ValueCopy(res)
}

// Put sets the value of a key within a single query transaction
func (bdb *badgerTransaction) Put(ctx context.Context, key, value []byte) error {
	err := bdb.Txn.Set(key, value)
	if err == badger.ErrKeyNotFound {
		err = ErrNotFound
	}
	return err
}

// Delete removes a key within a single transaction
func (bdb *badgerTransaction) Delete(ctx context.Context, key []byte) error {
	err := bdb.Txn.Delete(key)
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	return err
}

func (bdb *badgerTransaction) Seek(ctx context.Context, StartKey []byte) (Iterator, error) {
	it := bdb.Txn.NewIterator(badger.DefaultIteratorOptions)
	it.Seek(StartKey)
	return &badgerIterator{
		it,
	}, nil
}

// Discard removes all sides effects of the transaction
func (bdb *badgerTransaction) Discard(ctx context.Context) error {
	bdb.Txn.Discard()
	return nil
}

// Commit persists all side effects of the transaction and returns an error if there are any conflics
func (bdb *badgerTransaction) Commit(ctx context.Context) error {
	return bdb.Txn.Commit()
}

// badgerIterator

// Next yeilds the next key-value in iterator. Key-values can not be re-used between iterations. Make sure top copy the values if you must.
func (it *badgerIterator) Next(ctx context.Context) (key, value []byte, err error) {
	if !it.Iterator.Valid() {
		return nil, nil, ErrNotFound
	}

	defer it.Iterator.Next()

	value, err = it.Iterator.Item().ValueCopy(value)
	return it.Iterator.Item().Key(), value, err
}

// Close must always be called to clean up iterators.
func (bdb *badgerIterator) Close() error {
	bdb.Iterator.Close()
	return nil
}
