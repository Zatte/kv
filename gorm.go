package kv

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mssql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
)

type GromKeyValue struct {
	gorm.Model
	Key []byte `gorm:"primary_key" sql:"key"`
	Val []byte `sql:"val"`
}

type GormDB struct {
	*gorm.DB
}

type gormTransaction struct {
	*GormDB
}

type gormIterator struct {
	*sql.Rows
}

func NewGormDbFromUrl(u *url.URL) (*GormDB, error) {
	var db *gorm.DB
	var err error
	passw, _ := u.User.Password()
	switch u.Scheme {
	case "postgres":
		db, err = gorm.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s", u.Host, u.Port(), u.User.Username(), u.Path, passw))
	case "mysql":
		db, err = gorm.Open("mysql", fmt.Sprintf("%s@%s/%s?%s", u.User.String(), u.Host, u.Path, u.RawQuery))
	case "sqlite3":
		p := strings.TrimPrefix(u.Path, "/")
		db, err = gorm.Open("sqlite3", p)
	case "sqlserver":
		fallthrough
	case "mssql":
		uCopy, err := url.Parse(u.String())
		if err != nil {
			return nil, err
		}
		uCopy.Scheme = "sqlserver"

		db, err = gorm.Open("mssql", uCopy.String())
	default:
		return nil, ErrInvalidDb
	}

	if err != nil {
		return nil, err
	}

	return NewGormFromDB(db)
}

func NewGormFromDB(db *gorm.DB) (*GormDB, error) {
	db.AutoMigrate(&GromKeyValue{})
	return &GormDB{
		db,
	}, nil
}

// gorm db

func (gdb *GormDB) Close() error {
	return gdb.DB.Close()
}

// Get gets the value of a key within a single query transaction
func (gdb *GormDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	kv := &GromKeyValue{}
	if result := gdb.DB.Where("key = ?", key).First(&kv); result.Error != nil {
		if gorm.IsRecordNotFoundError(result.Error) {
			return nil, ErrNotFound
		}
		return nil, result.Error
	}

	return kv.Val, nil
}

// Put sets the value of a key within a single query transaction
func (gdb *GormDB) Put(ctx context.Context, key, value []byte) error {
	kv := &GromKeyValue{
		Key: key,
		Val: value,
	}
	if result := gdb.DB.Save(&kv); result.Error != nil {
		if gorm.IsRecordNotFoundError(result.Error) {
			return ErrNotFound
		}
		return result.Error
	}

	return nil
}

// Delete removes a key within a single transaction
func (gdb *GormDB) Delete(ctx context.Context, key []byte) error {
	kv := &GromKeyValue{
		Key: key,
	}
	if result := gdb.DB.Where("key = ?", key).Delete(&kv); result.Error != nil {
		if gorm.IsRecordNotFoundError(result.Error) {
			return ErrNotFound
		}
		return result.Error
	}

	return nil
}

// NewTransaction for batching multiple values inside a transaction
func (gdb *GormDB) NewTransaction(ctx context.Context, readOnly bool) (OrderedTransaction, error) {
	return &gormTransaction{
		&GormDB{
			gdb.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: readOnly}),
		},
	}, nil
}

// gormTransaction

// Seeks initializes an iterator at the given key (inclusive)
func (gdb *gormTransaction) Seek(ctx context.Context, StartKey []byte) (Iterator, error) {
	rows, err := gdb.DB.Model(&GromKeyValue{}).Select("key, val").Order("key").Where("key >= ?", StartKey).Rows()
	return &gormIterator{rows}, err
}

// Discard removes all sides effects of the transaction
func (gdb *gormTransaction) Discard(ctx context.Context) error {
	e := gdb.DB.Rollback()
	return e.Error
}

// Commit persists all side effects of the transaction and returns an error if there are any conflics
func (gdb *gormTransaction) Commit(ctx context.Context) error {
	e := gdb.DB.Commit()
	return e.Error
}

// gormIterator

// Next yeilds the next key-value in iterator. Key-values can not be re-used between iterations. Make sure top copy the values if you must.
func (it *gormIterator) Next(ctx context.Context) (key, value []byte, err error) {
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
func (gdb *gormIterator) Close() error {
	return nil
}
