package autoai

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

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
		w.Interval = 5 * time.Second
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
	if w.Control.AnalyzeActive() {
		return
	}
	tender, err := w.Store.NextTenderReadyForAI(ctx)
	if err != nil {
		return
	}
	if err := w.Store.MarkAnalyzing(ctx, tender.ID); err != nil {
		return
	}
	w.Log.Printf("auto-ai: start %s", tender.RegNumber)
	if err := AnalyzeTender(ctx, w.Store, w.Control, w.Analizator, tender, ""); err != nil {
		w.Log.Printf("auto-ai: %s error: %v", tender.RegNumber, err)
		return
	}
	w.Log.Printf("auto-ai: done %s", tender.RegNumber)
}

// AnalyzeTender — общая логика ручного и авто-анализа.
func AnalyzeTender(ctx context.Context, st *store.Store, ctrl *control.Controller, az *analizator.Client, t *store.Tender, checklistID string) error {
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
	if len(names) > 0 {
		corpus = "Документы в анализе (" + strconv.Itoa(len(names)) + "):\n- " + strings.Join(names, "\n- ") +
			"\n\nПосле всех порций документов дай ИТОГОВЫЙ ответ: да или нет (участие самозанятого) и почему.\n\n" + corpus
	}

	_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "analyzing"})

	analyzeCtx, ok := ctrl.BeginAnalyze(ctx)
	if !ok {
		return errBusy
	}
	defer ctrl.EndAnalyze()

	res, err := az.Analyze(analyzeCtx, analizator.AnalyzeRequest{
		RegNumber:   t.RegNumber,
		Text:        corpus,
		ChecklistID: checklistID,
		Title:       t.ObjectName,
	})
	if err != nil {
		if analyzeCtx.Err() != nil {
			_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "none"})
			return analyzeCtx.Err()
		}
		_, _ = st.UpdateTender(ctx, t.ID, map[string]any{"analysis_status": "other"})
		return err
	}

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
