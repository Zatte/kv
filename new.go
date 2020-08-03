package kv

import (
	"net/url"
)

// New opens up a new db based on a connection string.
func New(connectionString string) (OrderedTransactional, error) {
	u, err := url.Parse(connectionString)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "datastore":
		return NewDatastoreDbFromUrl(u)
	case "badger":
		return NewBadgerDbFromUrl(u)
	default:
		return NewGormDbFromUrl(u)
	}
}
