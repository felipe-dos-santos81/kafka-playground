package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNewID(t *testing.T) {
	a, b := NewID(), NewID()
	if !flowIDRe.MatchString(a) || a == b {
		t.Fatalf("want two distinct 8-hex ids, got %q %q", a, b)
	}
}

func TestStoreRoundTrip(t *testing.T) {
	s := Store{dir: t.TempDir()}
	f := clone(good)
	f.ID = NewID()
	if err := s.Put(f); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(f.ID)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(f)
	have, _ := json.Marshal(got)
	if string(want) != string(have) {
		t.Fatalf("round trip changed the flow:\n%s\n%s", want, have)
	}
	list, err := s.List()
	if err != nil || len(list) != 1 || list[0].ID != f.ID {
		t.Fatalf("want one listed flow, got %v %v", list, err)
	}
	entries, _ := os.ReadDir(s.dir)
	if len(entries) != 1 || entries[0].Name() != f.ID+".json" {
		t.Fatalf("want only %s.json on disk, got %v", f.ID, entries)
	}
	if err := s.Delete(f.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(f.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after delete, got %v", err)
	}
	if err := s.Delete(f.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound on second delete, got %v", err)
	}
}

func TestStoreListSkipsBrokenFile(t *testing.T) {
	s := Store{dir: t.TempDir()}
	f := clone(good)
	f.ID = NewID()
	if err := s.Put(f); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(s.dir, "deadbeef.json"), []byte("{not json"), 0o644)
	os.WriteFile(filepath.Join(s.dir, "notes.txt"), []byte("ignored"), 0o644)
	list, err := s.List()
	if err != nil || len(list) != 1 || list[0].ID != f.ID {
		t.Fatalf("want the one good flow, got %v %v", list, err)
	}
}

func TestStoreRejectsBadID(t *testing.T) {
	s := Store{dir: t.TempDir()}
	for _, id := range []string{"../etc/passwd", "0a1b2c3d.json", "ABCDEF12", "", "0a1b2c3d4"} {
		if _, err := s.Get(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%q): want ErrNotFound, got %v", id, err)
		}
		if err := s.Delete(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("Delete(%q): want ErrNotFound, got %v", id, err)
		}
	}
}

func TestStoreGetNormalizesMissingLists(t *testing.T) {
	s := Store{dir: t.TempDir()}
	os.WriteFile(filepath.Join(s.dir, "0a1b2c3d.json"), []byte(`{"name":"t"}`), 0o644)
	f, err := s.Get("0a1b2c3d")
	if err != nil || f.Nodes == nil || f.Edges == nil {
		t.Fatalf("want non-nil empty lists, got %+v %v", f, err)
	}
	b, _ := json.Marshal(f)
	if !strings.Contains(string(b), `"nodes":[]`) || !strings.Contains(string(b), `"edges":[]`) {
		t.Fatalf("want [] in %s", b)
	}
}
