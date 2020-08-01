package kv

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/jinzhu/badger"
	_ "github.com/jinzhu/badger/dialects/mssql"
	_ "github.com/jinzhu/badger/dialects/postgres"
	_ "github.com/jinzhu/badger/dialects/sqlite"
)


type badgerDB struct {
	*badger.DB
}

type badgerTransaction struct {
	*badgerDB
}

type badgerIterator struct {
	*sql.Rows
}

func NewbadgerDbFromUrl(u *url.URL) (*badgerDB, error) {
	var db *badger.DB
	var err error
	passw, _ := u.User.Password()

	

	if err != nil {
		return nil, err
	}

	return NewbadgerFromDB(db)
}

func NewbadgerFromDB(db *badger.DB) (*badgerDB, error) {
	db.AutoMigrate(&badgerKeyValue{})
	return &badgerDB{
		db,
	}, nil
}

// badger db

// Get gets the value of a key within a single query transaction
func (gdb *badgerDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	kv := &badgerKeyValue{}
	if result := gdb.DB.Where("key = ?", key).First(&kv); result.Error != nil {
		if badger.IsRecordNotFoundError(result.Error) {
			return nil, ErrNotFound
		}
		return nil, result.Error
	}

	return kv.Val, nil
}

// Put sets the value of a key within a single query transaction
func (gdb *badgerDB) Put(ctx context.Context, key, value []byte) error {
	kv := &badgerKeyValue{
		Key: key,
		Val: value,
	}
	if result := gdb.DB.Save(&kv); result.Error != nil {
		if badger.IsRecordNotFoundError(result.Error) {
			return ErrNotFound
		}
		return result.Error
	}

	return nil
}

// Delete removes a key within a single transaction
func (gdb *badgerDB) Delete(ctx context.Context, key []byte) error {
	kv := &badgerKeyValue{
		Key: key,
	}
	if result := gdb.DB.Where("key = ?", key).Delete(&kv); result.Error != nil {
		if badger.IsRecordNotFoundError(result.Error) {
			return ErrNotFound
		}
		return result.Error
	}

	return nil
}

// NewTransaction for batching multiple values inside a transaction
func (gdb *badgerDB) NewTransaction(ctx context.Context, readOnly bool) (OrderedTransaction, error) {
	return &badgerTransaction{
		&badgerDB{
			gdb.DB.BeginTx(ctx, &sql.TxOptions{}),
		},
	}, nil
}

// badgerTransaction

// Seeks initializes an iterator at the given key (inclusive)
func (gdb *badgerTransaction) Seek(ctx context.Context, StartKey []byte) (Iterator, error) {
	rows, err := gdb.DB.Model(&badgerKeyValue{}).Select("key, val").Order("key").Where("key >= ?", StartKey).Rows()
	return &badgerIterator{rows}, err
}

// Discard removes all sides effects of the transaction
func (gdb *badgerTransaction) Discard(ctx context.Context) error {
	e := gdb.DB.Rollback()
	return e.Error
}

// Commit persists all side effects of the transaction and returns an error if there are any conflics
func (gdb *badgerTransaction) Commit(ctx context.Context) error {
	e := gdb.DB.Commit()
	return e.Error
}

// badgerIterator

// Next yeilds the next key-value in iterator. Key-values can not be re-used between iterations. Make sure top copy the values if you must.
func (it *badgerIterator) Next(ctx context.Context) (key, value []byte, err error) {
	if !it.Rows.Next() {
		return nil, nil, ErrNotFound
	}

	var k, v []byte
	if err := it.Rows.Scan(&k, &v); err != nil {
		return nil, nil, err
	}

	if k == nil {
		return nil, nil, ErrNotFound
	}

	return k, v, nil
}

// Close must always be called to clean up iterators.
func (gdb *badgerIterator) Close() error {
	return nil
}
