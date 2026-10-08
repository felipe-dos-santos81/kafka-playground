// Topic owner: one long-running container per topic. ROLE=main owns
// <base>-<instance>, retry owns <base>-<instance>__retry and runs the
// redelivery worker (redeliver.go), dlq owns <base>-<instance>__dlq. Each
// creates its topic, keeps it in the desired state (reconcile.go) and serves
// /healthz and /metrics (metrics.go) on :9000.
// `topic-owner -healthcheck` is the compose healthcheck (the scratch image has
// no curl); it prints why the topic is not in its desired state.
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const addr = ":9000"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheck("http://localhost" + addr + "/healthz"))
	}
	cfg, err := loadConfig(os.Environ())
	if err != nil {
		log.Fatalf("topic-owner: %v", err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		log.Fatalf("topic-owner: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	o := newOwner(cfg, adm)
	reg := prometheus.NewRegistry()
	reg.MustRegister(newCollector(cfg, adm, func() bool { return o.health() == "" }))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	var wg sync.WaitGroup // what must finish before exit: the reconcile loop, the worker
	wg.Go(func() { o.run(ctx) })
	if cfg.Role == RoleRetry {
		w, err := newWorker(cfg, reg)
		if err != nil {
			log.Fatalf("topic-owner: %v", err)
		}
		wg.Go(func() { w.run(ctx) })
	}
	srv := &http.Server{Addr: addr, Handler: routes(o.health, reg)}
	shutDown := make(chan struct{})
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
		close(shutDown)
	}()
	log.Printf("%s: owner (role %s) on %s", cfg.Topic(), cfg.Role, addr)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("topic-owner: %v", err)
	}
	<-shutDown // Shutdown has drained the open requests
	wg.Wait()  // the worker has committed what it marked and left its group
}

// routes serves GET /healthz (200 "ok", or 503 and why not) and GET /metrics.
func routes(health func() string, reg *prometheus.Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if problem := health(); problem != "" {
			http.Error(w, problem, http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ok")
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return mux
}

// healthcheck fetches url and prints its body; its result is the exit code:
// 0 on 200, else 1. Docker keeps the output in .State.Health.Log.
func healthcheck(url string) int {
	c := http.Client{Timeout: 2 * time.Second}
	r, err := c.Get(url)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	defer r.Body.Close()
	io.Copy(os.Stdout, r.Body)
	if r.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
