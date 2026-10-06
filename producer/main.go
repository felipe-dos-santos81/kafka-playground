// Producer page: pick a topic (listed from the broker), optional key, JSON
// value; the browser validates the JSON, the server validates it again and
// answers with the partition and offset of the produced record, or the error.
package main

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

//go:embed index.html
var page embed.FS

const timeout = 10 * time.Second

type result struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func main() {
	// `producer -healthcheck` is the compose healthcheck: the scratch image has no curl.
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		r, err := http.Get("http://localhost:8081/api/topics")
		if err != nil || r.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		log.Fatal("KAFKA_BROKERS is required")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		log.Fatal(err)
	}
	adm := kadm.NewClient(cl)

	http.Handle("GET /", http.FileServerFS(page))

	http.HandleFunc("GET /api/topics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		topics, err := adm.ListTopics(ctx) // internal topics excluded
		if err != nil {
			fail(w, http.StatusBadGateway, err.Error())
			return
		}
		reply(w, http.StatusOK, topics.Names()) // sorted
	})

	// POST /api/produce?topic=orders&key=k1 with the JSON value as the raw body.
	http.HandleFunc("POST /api/produce", func(w http.ResponseWriter, r *http.Request) {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			fail(w, http.StatusBadRequest, "topic is required")
			return
		}
		value, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			fail(w, http.StatusBadRequest, err.Error())
			return
		}
		if !json.Valid(value) {
			fail(w, http.StatusBadRequest, "value is not valid JSON")
			return
		}
		rec := &kgo.Record{Topic: topic, Value: value}
		if key := r.URL.Query().Get("key"); key != "" {
			rec.Key = []byte(key)
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			fail(w, http.StatusBadGateway, err.Error())
			return
		}
		reply(w, http.StatusOK, result{Topic: topic, Partition: rec.Partition, Offset: rec.Offset})
	})

	log.Println("producer page on :8081, brokers", brokers)
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, msg string) {
	reply(w, status, map[string]string{"error": msg})
}
