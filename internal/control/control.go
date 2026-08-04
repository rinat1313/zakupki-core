// Package control — пауза/стоп сбора и вкл/выкл авто-AI.
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

// Controller управляет ingest и авто-AI независимо.
type Controller struct {
	ingestPaused  atomic.Bool
	ingestStopped atomic.Bool
	autoAI        atomic.Bool // авто-анализ включён

	mu            sync.Mutex
	analyzeCancel context.CancelFunc
	analyzeActive atomic.Bool
}

func New() *Controller { return &Controller{} }

func (c *Controller) IngestAllowsWork() bool {
	return !c.ingestPaused.Load() && !c.ingestStopped.Load()
}

func (c *Controller) AutoAIEnabled() bool { return c.autoAI.Load() }

func (c *Controller) SetAutoAI(on bool) {
	c.autoAI.Store(on)
	if !on {
		c.cancelAnalyzeLocked()
	}
}

func (c *Controller) PauseIngest() {
	c.ingestPaused.Store(true)
	c.ingestStopped.Store(false)
}

func (c *Controller) ResumeIngest() {
	c.ingestPaused.Store(false)
	c.ingestStopped.Store(false)
}

func (c *Controller) StopIngest() {
	c.ingestPaused.Store(true)
	c.ingestStopped.Store(true)
}

// BeginAnalyze — ручной или авто-анализ. Не зависит от autoAI (ручной всегда можно).
// cancelPrev — отменить текущий, если уже идёт.
func (c *Controller) BeginAnalyze(parent context.Context) (ctx context.Context, ok bool) {
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
	c.cancelAnalyzeLocked()
}

func (c *Controller) StopAnalyze() {
	c.cancelAnalyzeLocked()
}

func (c *Controller) cancelAnalyzeLocked() {
	c.mu.Lock()
	cancel := c.analyzeCancel
	c.analyzeCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.analyzeActive.Store(false)
}

func (c *Controller) AnalyzeActive() bool { return c.analyzeActive.Load() }

func (c *Controller) Status() map[string]any {
	ingest := StateRunning
	if c.ingestStopped.Load() {
		ingest = StateStopped
	} else if c.ingestPaused.Load() {
		ingest = StatePaused
	}
	return map[string]any{
		"ingest":         ingest,
		"auto_ai":        c.autoAI.Load(),
		"analyze_active": c.analyzeActive.Load(),
	}
}
