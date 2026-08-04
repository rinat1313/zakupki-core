// Package control — пауза/стоп воркеров сбора и AI-анализа.
package control

import (
	"context"
	"sync"
	"sync/atomic"
)

const (
	StateRunning = "running"
	StatePaused  = "paused"
	StateStopped = "stopped"
)

// Controller управляет ingest и analyze независимо.
type Controller struct {
	ingestPaused  atomic.Bool
	ingestStopped atomic.Bool
	analyzePaused atomic.Bool

	mu            sync.Mutex
	analyzeCancel context.CancelFunc
	analyzeActive atomic.Bool
}

func New() *Controller { return &Controller{} }

func (c *Controller) IngestAllowsWork() bool {
	return !c.ingestPaused.Load() && !c.ingestStopped.Load()
}

func (c *Controller) AnalyzeAllowsStart() bool {
	return !c.analyzePaused.Load()
}

func (c *Controller) PauseIngest() {
	c.ingestPaused.Store(true)
	c.ingestStopped.Store(false)
}

func (c *Controller) ResumeIngest() {
	c.ingestPaused.Store(false)
	c.ingestStopped.Store(false)
}

// StopIngest ставит паузу и помечает режим stop (очередь чистит API/store).
func (c *Controller) StopIngest() {
	c.ingestPaused.Store(true)
	c.ingestStopped.Store(true)
}

func (c *Controller) PauseAnalyze() { c.analyzePaused.Store(true) }

func (c *Controller) ResumeAnalyze() { c.analyzePaused.Store(false) }

// StopAnalyze отменяет текущий AI-запрос и ставит паузу на новые.
func (c *Controller) StopAnalyze() {
	c.analyzePaused.Store(true)
	c.mu.Lock()
	cancel := c.analyzeCancel
	c.analyzeCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// BeginAnalyze возвращает ctx, который можно отменить через StopAnalyze.
// ok=false если анализ на паузе.
func (c *Controller) BeginAnalyze(parent context.Context) (ctx context.Context, ok bool) {
	if c.analyzePaused.Load() {
		return parent, false
	}
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	if c.analyzeCancel != nil {
		c.analyzeCancel()
	}
	c.analyzeCancel = cancel
	c.mu.Unlock()
	c.analyzeActive.Store(true)
	return ctx, true
}

func (c *Controller) EndAnalyze() {
	c.analyzeActive.Store(false)
	c.mu.Lock()
	cancel := c.analyzeCancel
	c.analyzeCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Controller) Status() map[string]any {
	ingest := StateRunning
	if c.ingestStopped.Load() {
		ingest = StateStopped
	} else if c.ingestPaused.Load() {
		ingest = StatePaused
	}
	analyze := StateRunning
	if c.analyzePaused.Load() {
		analyze = StatePaused
	}
	return map[string]any{
		"ingest":         ingest,
		"analyze":        analyze,
		"analyze_active": c.analyzeActive.Load(),
	}
}
