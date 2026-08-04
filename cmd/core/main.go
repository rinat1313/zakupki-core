package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rinat1313/zakupki-core/internal/analizator"
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

	parser := parserclient.New(os.Getenv("PARSER_URL"))
	if parser.Enabled() {
		log.Printf("parser client: %s", parser.BaseURL)
	} else {
		log.Printf("parser client: disabled (set PARSER_URL)")
	}
	ctrl := control.New()
	w := &ingest.Worker{Store: st, Parser: parser, Control: ctrl, Log: log.Default()}
	go w.Run(ctx)

	az := analizator.New(os.Getenv("ANALIZATOR_URL"))
	if az.Enabled() {
		log.Printf("analizator: %s", az.BaseURL)
	} else {
		log.Printf("analizator: disabled")
	}
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
