package sqlite

import (
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	s, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSetGetOverwriteDelete(t *testing.T) {
	// Arrange
	s := newTestStore(t)

	// Act
	_, missingFound, missingErr := s.Get("nope")
	setErr := s.Set("k", "v1")
	v1, foundV1, errV1 := s.Get("k")
	overwriteErr := s.Set("k", "v2")
	v2, _, _ := s.Get("k")
	delErr := s.Delete("k")
	_, foundAfterDelete, _ := s.Get("k")

	// Assert
	if missingErr != nil || missingFound {
		t.Fatalf("Get(missing) = _, %v, %v; want _, false, nil", missingFound, missingErr)
	}
	if setErr != nil {
		t.Fatalf("Set: %v", setErr)
	}
	if errV1 != nil || !foundV1 || v1 != "v1" {
		t.Fatalf("Get(k) = %q, %v, %v; want \"v1\", true, nil", v1, foundV1, errV1)
	}
	if overwriteErr != nil {
		t.Fatalf("Set overwrite: %v", overwriteErr)
	}
	if v2 != "v2" {
		t.Fatalf("Get(k) after overwrite = %q; want \"v2\"", v2)
	}
	if delErr != nil {
		t.Fatalf("Delete: %v", delErr)
	}
	if foundAfterDelete {
		t.Fatalf("Get(k) after delete: found=true; want false")
	}
}
