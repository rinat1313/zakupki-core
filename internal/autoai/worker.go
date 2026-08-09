package autoai

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rinat1313/zakupki-core/internal/analizator"
	"github.com/rinat1313/zakupki-core/internal/control"
	"github.com/rinat1313/zakupki-core/internal/store"
)

// Worker периодически берёт готовые карточки и шлёт в analizator, если auto_ai включён.
type Worker struct {
	Store      *store.Store
	Control    *control.Controller
	Analizator *analizator.Client
	Log        *log.Logger
	Interval   time.Duration
}

func (w *Worker) Run(ctx context.Context) {
	if w.Log == nil {
		w.Log = log.Default()
	}
	if w.Interval <= 0 {
		w.Interval = 3 * time.Second
	}
	t := time.NewTicker(w.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick(ctx)
		}
	}
}

func (w *Worker) tick(ctx context.Context) {
	if w.Control == nil || !w.Control.AutoAIEnabled() {
		return
	}
	if w.Analizator == nil || !w.Analizator.Enabled() {
		return
	}
	free := w.Control.FreeAnalyzeSlots()
	if free <= 0 {
		return
	}
	for i := 0; i < free; i++ {
		analyzeCtx, release, ok := w.Control.BeginAnalyze(ctx)
		if !ok {
			return
		}
		tender, err := w.Store.NextTenderReadyForAI(ctx)
		if err != nil {
			release()
			if !errors.Is(err, store.ErrNotFound) {
				w.Log.Printf("auto-ai: next tender: %v", err)
			}
			return
		}
		if err := w.Store.MarkAnalyzing(ctx, tender.ID); err != nil {
			release()
			w.Log.Printf("auto-ai: claim %s: %v", tender.RegNumber, err)
			continue
		}
		t := tender
		rel := release
		aCtx := analyzeCtx
		w.Log.Printf("auto-ai: start %s (parallel %d)", t.RegNumber, i+1)
		go func() {
			opt := &AnalyzeOptions{PreCtx: aCtx, Release: rel}
			if err := AnalyzeTender(ctx, w.Store, w.Control, w.Analizator, t, opt); err != nil {
				w.Log.Printf("auto-ai: %s error: %v", t.RegNumber, err)
				return
			}
			w.Log.Printf("auto-ai: done %s", t.RegNumber)
		}()
	}
}

// AnalyzeOptions — опциональный чек-лист / конфиг.
type AnalyzeOptions struct {
	ConfigID    string
	ChecklistID string
	PreCtx      context.Context
	Release     func()
}

// AnalyzeTender — общая логика ручного и авто-анализа.
func AnalyzeTender(ctx context.Context, st *store.Store, ctrl *control.Controller, az *analizator.Client, t *store.Tender, opt *AnalyzeOptions) error {
	docs, err := st.ListDocuments(ctx, t.ID, true)
	if err != nil {
		return err
	}
	texts := make([]string, 0, len(docs))
	var names []string
	for _, d := range docs {
		if d.TextContent != nil && strings.TrimSpace(*d.TextContent) != "" {
			label := d.Filename
			if label == "" {
				label = d.UID
			}
			names = append(names, label)
			texts = append(texts, "### Документ: "+label+"\n"+strings.TrimSpace(*d.TextContent))
		}
	}
	corpus := analizator.BuildCorpus(t.ObjectName, t.Law, t.Status, t.NMCK, texts)
	if corpus == "" {
		_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "other"})
		return errNoText
	}

	cfg, _ := st.ActiveAIConfigForTender(ctx, t.ID)
	if opt != nil && strings.TrimSpace(opt.ConfigID) != "" {
		if id, perr := uuid.Parse(opt.ConfigID); perr == nil {
			if c, gerr := st.GetAIConfig(ctx, id); gerr == nil {
				cfg = c
			}
		}
	}

	focus := "Оцени закупку по возможности участия самозанятого"
	if cfg != nil && strings.TrimSpace(cfg.UserPrompt) != "" {
		focus = strings.TrimSpace(cfg.UserPrompt)
	}
	if len(names) > 0 {
		corpus = "Документы в анализе (" + strconv.Itoa(len(names)) + "):\n- " + strings.Join(names, "\n- ") +
			"\n\nПосле всех порций документов дай ИТОГОВЫЙ ответ по целевому вопросу и почему.\n\n" + corpus
	}

	startPct := 5
	_ = st.SetTenderProgress(ctx, t.ID, nil, &startPct)
	_, _ = st.RetainTender(ctx, t.ID, store.RetainReasonAnalyzing)
	_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "analyzing"})

	var analyzeCtx context.Context
	var release func()
	if opt != nil && opt.PreCtx != nil && opt.Release != nil {
		analyzeCtx = opt.PreCtx
		release = opt.Release
	} else {
		var ok bool
		analyzeCtx, release, ok = ctrl.BeginAnalyze(ctx)
		if !ok {
			return errBusy
		}
	}
	defer release()

	req := analizator.AnalyzeRequest{
		RegNumber:  t.RegNumber,
		Text:       corpus,
		Title:      t.ObjectName,
		UserPrompt: focus,
	}
	if opt != nil {
		req.ChecklistID = opt.ChecklistID
	}
	if cfg != nil {
		req.ChecklistID = cfg.ID.String()
		req.ConfigName = cfg.Name
		req.SystemPrompt = cfg.SystemPrompt
		req.UserPrompt = cfg.UserPrompt
		req.Rules = cfg.Rules
		if strings.TrimSpace(req.UserPrompt) == "" {
			req.UserPrompt = focus
		}
	}

	progCtx, progCancel := context.WithCancel(analyzeCtx)
	defer progCancel()
	go func() {
		ticker := time.NewTicker(700 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-progCtx.Done():
				return
			case <-ticker.C:
				p, err := az.Progress(progCtx, t.RegNumber)
				if err != nil || p == nil {
					continue
				}
				pct := p.Percent
				if pct < 5 {
					pct = 5
				}
				if pct > 95 {
					pct = 95
				}
				_ = st.SetTenderProgress(context.Background(), t.ID, nil, &pct)
			}
		}
	}()

	res, err := az.Analyze(analyzeCtx, req)
	progCancel()
	if err != nil {
		if analyzeCtx.Err() != nil {
			_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "none"})
			return analyzeCtx.Err()
		}
		_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "other"})
		return err
	}

	done := 100
	_ = st.SetTenderProgress(ctx, t.ID, nil, &done)

	score := res.Score
	summary := res.Summary
	if summary == "" && res.Recommendation != "" {
		summary = recommendationRU(res.Recommendation)
	}
	if res.Error != "" && summary == "" {
		summary = res.Error
	}
	_, err = st.UpsertAssessment(ctx, store.Assessment{
		TenderID: t.ID,
		Summary:  summary,
		Score:    &score,
		Details:  analizator.AssessmentDetails(res),
	})
	if err != nil {
		return err
	}
	stStatus := "analyzed"
	if res.Status == "failed" {
		stStatus = "other"
	}
	_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": stStatus})
	switch strings.ToLower(strings.TrimSpace(res.Recommendation)) {
	case "participate", "caution":
		_, _ = st.RetainTender(ctx, t.ID, store.RetainReasonAIInteresting)
	default:
		// already retained when analysis started
	}
	return nil
}

func recommendationRU(rec string) string {
	switch strings.ToLower(strings.TrimSpace(rec)) {
	case "participate":
		return "Да — самозанятому целесообразно участвовать."
	case "caution":
		return "С оговорками — участие возможно, но есть риски."
	case "skip":
		return "Нет — самозанятому не стоит / нельзя участвовать."
	default:
		return "Недостаточно данных для однозначного ответа."
	}
}

var errNoText = fmt.Errorf("нет текста документов для анализа")
var errBusy = fmt.Errorf("анализ уже выполняется")
