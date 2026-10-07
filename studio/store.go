// Store keeps one JSON file per flow under dir: <id>.json, written through a
// temp file and os.Rename so a reader never sees a half-written flow.
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type Store struct{ dir string }

var ErrNotFound = errors.New("flow not found")

var flowIDRe = regexp.MustCompile(`^[0-9a-f]{8}$`)

// NewID returns 8 random lowercase hex characters.
func NewID() string {
	b := make([]byte, 4)
	rand.Read(b) // cannot fail on supported platforms
	return hex.EncodeToString(b)
}

// path is the only way to a flow file: an id that is not 8 lowercase hex
// characters (so no "..", no separators) is ErrNotFound.
func (s Store) path(id string) (string, error) {
	if !flowIDRe.MatchString(id) {
		return "", ErrNotFound
	}
	return filepath.Join(s.dir, id+".json"), nil
}

// List returns every readable flow sorted by name; a file that does not parse
// is logged and skipped so one hand-edited mistake does not hide the rest.
func (s Store) List() ([]Flow, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	flows := []Flow{}
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), ".json")
		if e.IsDir() || !ok {
			continue
		}
		f, err := s.Get(id)
		if err != nil {
			log.Printf("skipping %s: %v", e.Name(), err)
			continue
		}
		flows = append(flows, f)
	}
	sort.Slice(flows, func(i, j int) bool { return flows[i].Name < flows[j].Name })
	return flows, nil
}

func (s Store) Get(id string) (Flow, error) {
	var f Flow
	path, err := s.path(id)
	if err != nil {
		return f, err
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return f, ErrNotFound
	}
	if err != nil {
		return f, err
	}
	if err := json.Unmarshal(b, &f); err != nil {
		return f, err
	}
	f.normalize()
	f.ID = id // the file name wins over whatever the file says
	return f, nil
}

func (s Store) Put(f Flow) error {
	path, err := s.path(f.ID)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, f.ID+".*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func (s Store) Delete(id string) error {
	path, err := s.path(id)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	return err
}
