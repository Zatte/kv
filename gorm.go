package kv

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type GromKeyValue struct {
	Key []byte `gorm:"primaryKey"`
	Val []byte
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
		dsn := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s", u.Host, u.Port(), u.User.Username(), u.Path, passw)
		db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
	case "mysql":
		dsn := fmt.Sprintf("%s@%s/%s?%s", u.User.String(), u.Host, u.Path, u.RawQuery)
		db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
	case "sqlite3":
		p := strings.TrimPrefix(u.Path, "/")
		db, err = gorm.Open(sqlite.Open(p), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
	case "sqlserver":
		fallthrough
	case "mssql":
		uCopy, err := url.Parse(u.String())
		if err != nil {
			return nil, err
		}
		uCopy.Scheme = "sqlserver"

		dsn := uCopy.String()
		db, err = gorm.Open(sqlserver.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
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
	d, err := gdb.DB.DB()
	if err != nil {
		return err
	}
	return d.Close()
}

// Get gets the value of a key within a single query transaction
func (gdb *GormDB) Get(ctx context.Context, key []byte) ([]byte, error) {
	kv := &GromKeyValue{}
	if result := gdb.DB.Where("key = ?", key).First(&kv); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
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

	result := gdb.DB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.Assignments(map[string]interface{}{"val": value}),
	}).Create(&kv)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
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
	result := gdb.DB.Where("key = ?", key).Delete(&kv)
	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return ErrNotFound
		}
		return result.Error
	}
	if result.RowsAffected <= 0 {
		return ErrNotFound
	}

	return nil
}

// NewTransaction for batching multiple values inside a transaction
func (gdb *GormDB) NewTransaction(ctx context.Context, readOnly bool) (OrderedTransaction, error) {
	return &gormTransaction{
		&GormDB{
			gdb.DB.Begin(&sql.TxOptions{ReadOnly: readOnly}),
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
