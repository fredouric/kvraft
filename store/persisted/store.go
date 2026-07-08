package persisted

import "sync"

type Store struct {
	mu    sync.Mutex
	store map[string]string
}

func New() *Store {
	return &Store{store: make(map[string]string)}
}

func (s *Store) Get(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store[key], nil
}

func (s *Store) Set(key string, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.store, key)
	return nil
}
