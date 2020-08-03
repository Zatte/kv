# KeyValue "kv"
Simple key-value interface backed by multiple backends (multiple sql engines through gorm, pure golang through badger DB; cloud scale through google datastore). 

This is not meant for the highest performance applications but rather quick prototyping for cases where simple get/upsert/del/iterate in enough to get started. The different backends are meants for various scaling scenarios  and deployment needs. 

## Status
Very much WIP/Early stage. PRs are welcome.

## Usage 

```golang
import "github.com/zatte/kv"

func main(){
  ctx  := context.Background()
  // gorm / sqlite3 in memory, limited transactional support. Not great for testing
  // db, err := kv.New("sqlite3:///file%3A%3Amemory%3A%3Fcache%3Dshared&mode=rwc")

  // gorm / sqlite3 in memory, limited transactional support. Not great for testing
  // db, err := kv.New("psql://user:password@host:port/database")
  // db, err := kv.New("mysql", "mysql://user:password@/dbname?charset=utf8&parseTime=True&loc=Local")
  // db, err := kv.New("mssql", "mssql://username:password@localhost:1433?database=dbname")

  // Badger DB In memory
  // db, err := kv.New("badger:///?memory=true")

  // Badger DB by path
  // db, err := kv.New("badger:///tmp/db")

  // datastore, local emulator for testing / development
  // db, err := kv.New("datastore://" + os.Getenv("DATASTORE_PROJECT_ID"))

  // datastore based on project id
  // db, err := kv.New("datastore://google-cloud-project-id")

  db, err := kv.New("badger:///?memory=true")

  // Put, Get, Del
  err := db.Put(ctx, []byte("key"), []byte("value"))
  key, err := db.Get(ctx, []byte("key"))
  err := db.Delete(ctx, []byte("key"))
  
  // Create a transaction
  tx, err := db.NewTransaction(ctx, false) // read only transactions. Not supported by all backends but some. 
  defer tx.Discard()

  // Same as above: Put, Get, Del
  err := tx.Put(ctx, []byte("key"), []byte("value"))
  key, err := tx.Get(ctx, []byte("key"))
  err := tx.Delete(ctx, []byte("key"))

  // but transactions can also be have iterators 
  it, err := tx.Seek(ctx, []byte("inclusvie_start_key_utf8_sort_order"))
  defer it.Close()

  for key, val, err := it.Next(); err == nil; key, val, err = it.Next() {
    // process keys and values in order.
  }
}

```

## Testing

Doesn't perform integration testing with external databaes except datastore.

``` shell
gcloud beta emulators datastore start & 
$(gcloud beta emulators datastore env-init)
go test
```

## TODO
- [ ] Add Create (failure on existing keys)
- [ ] Add Backend Redis
- [ ] Improve errors (atm all errors are ErrNotFound)
- [ ] Improve docs
- [ ] Integration testing with 
- - [ ] MySQL
- - [ ] Postgres
- - [ ] MsSQL