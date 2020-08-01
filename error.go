package kv

type KvError string

func (e KvError) Error() string {
	return string(e)
}

const (
	ErrInvalidDb KvError = "no supported database type"
	ErrNotFound  KvError = "record not found"
)
