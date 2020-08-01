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
	default:
		return NewGormDbFromUrl(u)
	}
}
