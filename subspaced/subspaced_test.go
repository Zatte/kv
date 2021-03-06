package subspaced

import (
	"context"
	"testing"

	"github.com/zatte/kv"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKvStores(t *testing.T) {
	badger, err := kv.New("badger:///?memory=true")
	require.NoError(t, err)

	testStores(t, New(badger, "something", 123, "darkside"), "test_basic_subspace")
	testStores(t, New(badger, "something", 123), "test_basic_subspace_conflict")
}

func testStores(t *testing.T, db kv.OrderedTransactional, name string) {
	ctx := context.Background()
	t.Run(name+": create delete read", func(t *testing.T) {
		v, err := db.Get(ctx, []byte("A0"))
		assert.Error(t, err, kv.ErrNotFound.Error())

		assert.NoError(t, db.Put(ctx, []byte("A0"), []byte("1")))
		assert.NoError(t, db.Put(ctx, []byte("A01"), []byte("2")))
		assert.NoError(t, db.Put(ctx, []byte("A02"), []byte("3")))
		assert.NoError(t, db.Put(ctx, []byte("A021"), []byte("4")))
		assert.NoError(t, db.Put(ctx, []byte("A022"), []byte("5")))

		assert.NoError(t, db.Delete(ctx, []byte("A02")))

		v, err = db.Get(ctx, []byte("A0"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("1"))

		v, err = db.Get(ctx, []byte("A01"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("2"))

		v, err = db.Get(ctx, []byte("A02"))
		assert.EqualError(t, err, kv.ErrNotFound.Error())

		v, err = db.Get(ctx, []byte("A021"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("4"))

		v, err = db.Get(ctx, []byte("A022"))
		assert.NoError(t, err)
		assert.Equal(t, v, []byte("5"))
	})

	t.Run(name+": adding and reading (iterator) ordered twice", func(t *testing.T) {
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
		defer t1.Discard(ctx)
		assert.NoError(t, err)
		t2, err := db.NewTransaction(ctx, false)
		defer t1.Discard(ctx)
		assert.NoError(t, err)

		it1, err := t1.Seek(ctx, []byte("B02"))
		assert.NoError(t, err)
		defer it1.Close()
		it2, err := t2.Seek(ctx, []byte("B02"))
		assert.NoError(t, err)
		defer it2.Close()

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
