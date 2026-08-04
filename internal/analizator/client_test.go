package analizator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBuildCorpus(t *testing.T) {
	nmck := 150000.5
	got := BuildCorpus("Поставка бумаги", "44-FZ", "Подача заявок", &nmck, []string{"  текст1  ", "", "текст2"})
	for _, part := range []string{"Поставка бумаги", "44-FZ", "150000.50", "текст1", "текст2"} {
		if !strings.Contains(got, part) {
			t.Fatalf("missing %q in:\n%s", part, got)
		}
	}
}

func TestAssessmentDetails(t *testing.T) {
	raw := AssessmentDetails(&Result{
		Status:         "completed",
		ChecklistID:    "quick",
		Recommendation: "caution",
		Score:          0.55,
		Summary:        "осторожно",
		Risks:          []string{"срок"},
	})
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["source"] != "analizator_zakupok" {
		t.Fatalf("source=%v", m["source"])
	}
	if m["recommendation"] != "caution" {
		t.Fatalf("recommendation=%v", m["recommendation"])
	}
}

func TestClientAnalyze(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/analyze" || r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(Result{
			RegNumber:      "123",
			Status:         "completed",
			ChecklistID:    "default",
			Recommendation: "participate",
			Score:          0.9,
			Summary:        "ok",
		})
	}))
	defer srv.Close()

	c := New(srv.URL)
	res, err := c.Analyze(context.Background(), AnalyzeRequest{RegNumber: "123", Text: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Recommendation != "participate" || res.Score != 0.9 {
		t.Fatalf("%+v", res)
	}
}

func TestClientDisabled(t *testing.T) {
	c := New("")
	if c.Enabled() {
		t.Fatal("expected disabled")
	}
	if _, err := c.Analyze(context.Background(), AnalyzeRequest{Text: "x"}); err == nil {
		t.Fatal("expected error")
	}
}
