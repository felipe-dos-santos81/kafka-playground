package main

import (
	"context"
	"errors"
	"testing"
)

// A lone topic validates for Deploy but has nothing to start; Deploy must say so
// before it touches Docker or Kafka (both nil here).
func TestDeployNothingToRun(t *testing.T) {
	e := &Engine{store: Store{dir: t.TempDir()}}
	f := Flow{ID: "0123abcd", Name: "lone", Nodes: []Node{
		node("topic-1", "topic", `{"name":"orders","partitions":3,"replication_factor":1}`),
	}}
	if err := e.store.Put(f); err != nil {
		t.Fatal(err)
	}
	var ps Problems
	if err := e.Deploy(context.Background(), f.ID); !errors.As(err, &ps) {
		t.Fatalf("want Problems, got %v", err)
	}
}
