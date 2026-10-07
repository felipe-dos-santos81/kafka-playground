package main

import (
	"context"
	"testing"
)

func TestDiscoverSelfFromEnv(t *testing.T) {
	t.Setenv("STUDIO_IMAGE", "kafka-playground/studio:0.1.0")
	t.Setenv("STUDIO_NETWORK", "kafka-playground_default")
	me, err := discoverSelf(context.Background(), nil) // both set: Docker is never asked
	if err != nil || me != (self{image: "kafka-playground/studio:0.1.0", network: "kafka-playground_default"}) {
		t.Fatalf("got %+v %v", me, err)
	}
}
