package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rinat1313/zakupki-core/internal/analizator"
	"github.com/rinat1313/zakupki-core/internal/autoai"
	"github.com/rinat1313/zakupki-core/internal/control"
	"github.com/rinat1313/zakupki-core/internal/httpapi"
	"github.com/rinat1313/zakupki-core/internal/ingest"
	"github.com/rinat1313/zakupki-core/internal/parserclient"
	"github.com/rinat1313/zakupki-core/internal/store"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := store.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	st := store.NewStore(pool)
	if n, err := st.RequeueStuckRunning(ctx); err != nil {
		log.Printf("requeue stuck: %v", err)
	} else if n > 0 {
		log.Printf("requeued %d stuck running ingest items", n)
	}
	if n, err := st.RequeueStuckAnalyzing(ctx); err != nil {
		log.Printf("requeue stuck analyzing: %v", err)
	} else if n > 0 {
		log.Printf("requeued %d stuck analyzing tenders", n)
	}

	parser := parserclient.New(os.Getenv("PARSER_URL"))
	if parser.Enabled() {
		log.Printf("parser client: %s", parser.BaseURL)
	} else {
		log.Printf("parser client: disabled (set PARSER_URL)")
	}
	ctrl := control.New()

	ingestWorkers := envInt("INGEST_WORKERS", 3)
	if ingestWorkers < 1 {
		ingestWorkers = 1
	}
	if ingestWorkers > 16 {
		ingestWorkers = 16
	}
	log.Printf("ingest workers: %d (FOR UPDATE SKIP LOCKED)", ingestWorkers)
	for i := 0; i < ingestWorkers; i++ {
		w := &ingest.Worker{Store: st, Parser: parser, Control: ctrl, Log: log.Default()}
		go w.Run(ctx)
	}

	az := analizator.New(os.Getenv("ANALIZATOR_URL"))
	if az.Enabled() {
		log.Printf("analizator: %s", az.BaseURL)
	} else {
		log.Printf("analizator: disabled")
	}

	auto := &autoai.Worker{Store: st, Control: ctrl, Analizator: az, Log: log.Default()}
	go auto.Run(ctx)

	srv := httpapi.New(st, az, ctrl)

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.WithCORS(srv.Mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("zakupki-core listening on %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	shCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
	defer c()
	_ = httpSrv.Shutdown(shCtx)
}

func envInt(k string, def int) int {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
