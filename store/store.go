package store

type Store interface {
	Get(key string) (string, bool, error)
	Set(key string, value string) error
	Delete(key string) error
}
