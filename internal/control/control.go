// Package control — пауза/стоп сбора и вкл/выкл авто-AI + слоты параллельного AI.
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
	autoAI        atomic.Bool

	capacity atomic.Int32
	active   atomic.Int32

	mu      sync.Mutex
	cancels map[uint64]context.CancelFunc
	nextID  uint64
}

func New() *Controller {
	c := &Controller{cancels: map[uint64]context.CancelFunc{}}
	c.capacity.Store(1)
	return c
}

func (c *Controller) IngestAllowsWork() bool {
	return !c.ingestPaused.Load() && !c.ingestStopped.Load()
}

func (c *Controller) AutoAIEnabled() bool { return c.autoAI.Load() }

func (c *Controller) SetAutoAI(on bool) {
	c.autoAI.Store(on)
	if !on {
		c.StopAnalyze()
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

func (c *Controller) SetAnalyzeCapacity(n int) {
	if n < 1 {
		n = 1
	}
	if n > 64 {
		n = 64
	}
	c.capacity.Store(int32(n))
}

func (c *Controller) AnalyzeCapacity() int     { return int(c.capacity.Load()) }
func (c *Controller) AnalyzeActiveCount() int  { return int(c.active.Load()) }
func (c *Controller) FreeAnalyzeSlots() int {
	free := int(c.capacity.Load()) - int(c.active.Load())
	if free < 0 {
		return 0
	}
	return free
}

// BeginAnalyze занимает один слот. release обязан быть вызван (обычно defer).
func (c *Controller) BeginAnalyze(parent context.Context) (ctx context.Context, release func(), ok bool) {
	for {
		cur := c.active.Load()
		capN := c.capacity.Load()
		if cur >= capN {
			return nil, nil, false
		}
		if c.active.CompareAndSwap(cur, cur+1) {
			break
		}
	}
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	c.cancels[id] = cancel
	c.mu.Unlock()

	var once sync.Once
	release = func() {
		once.Do(func() {
			cancel()
			c.mu.Lock()
			delete(c.cancels, id)
			c.mu.Unlock()
			c.active.Add(-1)
			if c.active.Load() < 0 {
				c.active.Store(0)
			}
		})
	}
	return ctx, release, true
}

// EndAnalyze — совместимость: no-op если используют release. Оставлен для старых вызовов.
func (c *Controller) EndAnalyze() {}

func (c *Controller) StopAnalyze() {
	c.mu.Lock()
	cancels := c.cancels
	c.cancels = map[uint64]context.CancelFunc{}
	c.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	// active уменьшится через release() при завершении горутин
}

func (c *Controller) AnalyzeActive() bool { return c.active.Load() > 0 }

func (c *Controller) Status() map[string]any {
	ingest := StateRunning
	if c.ingestStopped.Load() {
		ingest = StateStopped
	} else if c.ingestPaused.Load() {
		ingest = StatePaused
	}
	return map[string]any{
		"ingest":           ingest,
		"auto_ai":          c.autoAI.Load(),
		"analyze_active":   c.active.Load() > 0,
		"analyze_running":  c.active.Load(),
		"analyze_capacity": c.capacity.Load(),
	}
}
