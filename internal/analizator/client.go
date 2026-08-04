// Package analizator — HTTP-клиент к микросервису analizator_zakupok (LM Studio).
package analizator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client вызывает REST API analizator_zakupok.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

func New(baseURL string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	return &Client{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Minute, // LLM map-reduce может быть долгим
		},
	}
}

func (c *Client) Enabled() bool {
	return c != nil && c.BaseURL != ""
}

// AnalyzeRequest — тело POST /api/v1/analyze.
type AnalyzeRequest struct {
	RegNumber    string `json:"reg_number,omitempty"`
	Text         string `json:"text,omitempty"`
	ChecklistID  string `json:"checklist_id,omitempty"`
	Title        string `json:"title,omitempty"`
	ConfigName   string `json:"config_name,omitempty"`
	SystemPrompt string `json:"system_prompt,omitempty"`
	UserPrompt   string `json:"user_prompt,omitempty"`
	Rules        string `json:"rules,omitempty"`
}

// ProgressInfo — прогресс дозированного анализа.
type ProgressInfo struct {
	RegNumber  string `json:"reg_number"`
	Percent    int    `json:"percent"`
	DosesDone  int    `json:"doses_done"`
	DosesTotal int    `json:"doses_total"`
	Phase      string `json:"phase"`
}

// ItemResult — пункт чек-листа.
type ItemResult struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Status   string   `json:"status"`
	Score    float64  `json:"score"`
	Findings string   `json:"findings"`
	Evidence []string `json:"evidence,omitempty"`
}

// Result — ответ analizator_zakupok.
type Result struct {
	RegNumber      string       `json:"reg_number"`
	Law            string       `json:"law,omitempty"`
	Status         string       `json:"status"`
	ChecklistID    string       `json:"checklist_id"`
	ChecklistName  string       `json:"checklist_name,omitempty"`
	Model          string       `json:"model,omitempty"`
	Recommendation string       `json:"recommendation,omitempty"`
	Score          float64      `json:"score"`
	Summary        string       `json:"summary,omitempty"`
	Items          []ItemResult `json:"items,omitempty"`
	Risks          []string     `json:"risks,omitempty"`
	Actions        []string     `json:"actions,omitempty"`
	Error          string       `json:"error,omitempty"`
	AnalyzedAt     string       `json:"analyzed_at,omitempty"`
}

type apiError struct {
	Error string `json:"error"`
}

// Ping проверяет GET /health.
func (c *Client) Ping(ctx context.Context) error {
	if !c.Enabled() {
		return fmt.Errorf("analizator URL not configured")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/health", nil)
	if err != nil {
		return err
	}
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		return fmt.Errorf("analizator health %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// Analyze вызывает POST /api/v1/analyze.
func (c *Client) Analyze(ctx context.Context, in AnalyzeRequest) (*Result, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("analizator URL not configured (set ANALIZATOR_URL)")
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/v1/analyze", bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		var ae apiError
		_ = json.Unmarshal(body, &ae)
		if ae.Error != "" {
			return nil, fmt.Errorf("analizator: %s", ae.Error)
		}
		return nil, fmt.Errorf("analizator HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	var out Result
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("analizator decode: %w", err)
	}
	return &out, nil
}

// Progress читает GET /api/v1/analyze/progress/{reg}.
func (c *Client) Progress(ctx context.Context, reg string) (*ProgressInfo, error) {
	if !c.Enabled() || strings.TrimSpace(reg) == "" {
		return nil, fmt.Errorf("no progress")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/api/v1/analyze/progress/"+reg, nil)
	if err != nil {
		return nil, err
	}
	res, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode >= 300 {
		return nil, fmt.Errorf("progress HTTP %d", res.StatusCode)
	}
	var out ProgressInfo
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// BuildCorpus собирает текст тендера из карточки и документов для передачи в LLM.
func BuildCorpus(objectName, law, status string, nmck *float64, docTexts []string) string {
	var b strings.Builder
	if objectName != "" {
		fmt.Fprintf(&b, "Объект закупки: %s\n", objectName)
	}
	if law != "" {
		fmt.Fprintf(&b, "Закон: %s\n", law)
	}
	if status != "" {
		fmt.Fprintf(&b, "Статус: %s\n", status)
	}
	if nmck != nil {
		fmt.Fprintf(&b, "НМЦК: %.2f\n", *nmck)
	}
	for i, t := range docTexts {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		fmt.Fprintf(&b, "\n--- документ %d ---\n%s\n", i+1, t)
	}
	return strings.TrimSpace(b.String())
}

// AssessmentDetails упаковывает ответ LLM в JSON для tender_assessments.details.
func AssessmentDetails(r *Result) json.RawMessage {
	if r == nil {
		return json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(map[string]any{
		"source":         "analizator_zakupok",
		"status":         r.Status,
		"checklist_id":   r.ChecklistID,
		"recommendation": r.Recommendation,
		"model":          r.Model,
		"items":          r.Items,
		"risks":          r.Risks,
		"actions":        r.Actions,
		"error":          r.Error,
		"analyzed_at":    r.AnalyzedAt,
	})
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}
