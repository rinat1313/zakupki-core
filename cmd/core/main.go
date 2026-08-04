package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
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

	// Пул воркеров: активных = max(1, 10% очереди), потолок 32.
	const maxSlots = 32
	var desired atomic.Int64
	desired.Store(1)
	for i := 0; i < maxSlots; i++ {
		id := i
		w := &ingest.Worker{Store: st, Parser: parser, Control: ctrl, Log: log.Default()}
		go func() {
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if int64(id) >= desired.Load() {
						continue
					}
					w.TickOnce(ctx)
				}
			}
		}()
	}
	go func() {
		t := time.NewTicker(3 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				n, err := st.CountQueuedIngest(ctx)
				if err != nil {
					continue
				}
				want := n / 10
				if want < 1 {
					want = 1
				}
				if want > maxSlots {
					want = maxSlots
				}
				prev := desired.Swap(int64(want))
				if prev != int64(want) {
					log.Printf("ingest workers desired=%d (queue=%d, 10%%)", want, n)
				}
			}
		}
	}()
	log.Printf("ingest worker pool: up to %d slots, scale = max(1, queue/10)", maxSlots)

	az := analizator.New(os.Getenv("ANALIZATOR_URL"))
	if az.Enabled() {
		log.Printf("analizator: %s", az.BaseURL)
	} else {
		log.Printf("analizator: disabled")
	}

	// Синхронизация ёмкости AI с пулом LM Studio.
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !az.Enabled() {
					ctrl.SetAnalyzeCapacity(1)
					continue
				}
				st, err := az.PoolStatus(ctx)
				if err != nil || st == nil {
					continue
				}
				n := st.MaxParallel
				if n < 1 {
					n = st.Healthy
				}
				if n < 1 {
					n = 1
				}
				ctrl.SetAnalyzeCapacity(n)
			}
		}
	}()

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
