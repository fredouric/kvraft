package memory

import (
	"maps"
	"sync"
)

type Store struct {
	mu    sync.Mutex
	store map[string]string
}

func New() *Store {
	return &Store{store: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.store[key]
	return v, ok, nil
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

func (s *Store) Dump() (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dump := make(map[string]string)
	maps.Copy(dump, s.store)
	return dump, nil
}

func (s *Store) Restore(snapshot map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	restored := make(map[string]string)
	maps.Copy(restored, snapshot)
	s.store = restored
	return nil
}
