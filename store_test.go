package kv

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKvStores(t *testing.T) {
	db, err := New("sqlite3:///file%3A%3Amemory%3A%3Fcache%3Dshared")
	assert.NoError(t, err)
	testStores(t, db)
}

func testStores(t *testing.T, db OrderedTransactional) {
	ctx := context.Background()
	t.Run("test CDR", func(t *testing.T) {
		assert.NoError(t, db.Put(ctx, []byte("A0"), []byte("1")))
		assert.NoError(t, db.Put(ctx, []byte("A01"), []byte("2")))
		assert.NoError(t, db.Put(ctx, []byte("A02"), []byte("3")))
		assert.NoError(t, db.Put(ctx, []byte("A021"), []byte("4")))
		assert.NoError(t, db.Put(ctx, []byte("A022"), []byte("5")))

		assert.NoError(t, db.Delete(ctx, []byte("A02")))

		v, err := db.Get(ctx, []byte("A0"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("1"))

		v, err = db.Get(ctx, []byte("A01"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("2"))

		v, err = db.Get(ctx, []byte("A02"))
		assert.EqualError(t, err, ErrNotFound.Error())

		v, err = db.Get(ctx, []byte("A021"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("4"))

		v, err = db.Get(ctx, []byte("A022"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("5"))
	})

	t.Run("test adding and reading back ordered twice", func(t *testing.T) {
		assert.NoError(t, db.Put(ctx, []byte("B0"), []byte("1")))
		assert.NoError(t, db.Put(ctx, []byte("B01"), []byte("2")))
		assert.NoError(t, db.Put(ctx, []byte("B02"), []byte("3")))
		assert.NoError(t, db.Put(ctx, []byte("B021"), []byte("4")))
		assert.NoError(t, db.Put(ctx, []byte("B022"), []byte("5")))
		assert.NoError(t, db.Put(ctx, []byte("B1"), []byte("6")))
		assert.NoError(t, db.Put(ctx, []byte("B12"), []byte("7")))
		assert.NoError(t, db.Put(ctx, []byte("B123"), []byte("8")))
		assert.NoError(t, db.Put(ctx, []byte("B124"), []byte("9")))

		t1, err := db.NewTransaction(ctx, false)
		assert.NoError(t, err)
		t2, err := db.NewTransaction(ctx, false)
		assert.NoError(t, err)

		it1, err := t1.Seek(ctx, []byte("B02"))
		assert.NoError(t, err)
		it2, err := t2.Seek(ctx, []byte("B02"))
		assert.NoError(t, err)

		var previousVal [3]byte
		_, v, err := it1.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "3", string(v))
		for ; err == nil; _, v, err = it1.Next(ctx) {
			//t.Logf("got value: %s (err:%v)", string(v), err)
			assert.Less(t, string(previousVal[:]), string(v))
			copy(v, previousVal[:])
		}

		_, v, err = it2.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, "3", string(v))
		for ; err == nil; _, v, err = it2.Next(ctx) {
			//t.Logf("got value: %s (err:%v)", string(v), err)
			assert.Less(t, string(previousVal[:]), string(v))
			copy(v, previousVal[:])
		}

	})
}
