// Pipeline Studio: build Kafka pipelines on a canvas and run them. `studio`
// serves the UI and API on :8082; `studio -healthcheck` is the compose
// healthcheck (the scratch image has no curl); `studio node` runs one node of
// a deployed flow (node.go).
package main

import (
	"context"
	"embed"
	"io/fs"
	"log"
	"net/http"
	"os"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// all: keeps the pattern valid while dist holds only .gitkeep.
//
//go:embed all:ui/dist
var uiFiles embed.FS

const addr = ":8082"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		r, err := http.Get("http://localhost" + addr + "/api/health")
		if err != nil || r.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "node" {
		runNode()
		return
	}
	dir := os.Getenv("STUDIO_DATA")
	if dir == "" {
		dir = "/data"
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal(err)
	}
	ui, err := fs.Sub(uiFiles, "ui/dist")
	if err != nil {
		log.Fatal(err)
	}
	cli, err := newDocker()
	if err != nil {
		log.Fatal(err)
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}
	kafka, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		log.Fatal(err)
	}
	store := Store{dir: dir}
	engine := &Engine{store: store, docker: cli, adm: kadm.NewClient(kafka), brokers: brokers}
	if err := engine.Reconcile(context.Background()); err != nil {
		log.Println("reconcile:", err) // health says why; orphans stay until the next start
	}
	s := &server{store: store, docker: cli, kafka: kafka, engine: engine}
	log.Println("studio on", addr, "flows in", dir)
	log.Fatal(http.ListenAndServe(addr, newMux(s, ui)))
}
