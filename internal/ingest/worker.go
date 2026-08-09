package ingest

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/rinat1313/zakupki-core/internal/control"
	"github.com/rinat1313/zakupki-core/internal/parserclient"
	"github.com/rinat1313/zakupki-core/internal/store"
)

type Worker struct {
	Store   *store.Store
	Parser  *parserclient.Client
	Control *control.Controller
	Log     *log.Logger
}

func (w *Worker) Run(ctx context.Context) {
	if w.Log == nil {
		w.Log = log.Default()
	}
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.TickOnce(ctx)
		}
	}
}

// TickOnce — одна попытка взять задачу (для динамического пула).
func (w *Worker) TickOnce(ctx context.Context) {
	w.tick(ctx)
}

func (w *Worker) tick(ctx context.Context) {
	if w.Control != nil && !w.Control.IngestAllowsWork() {
		return
	}
	item, job, err := w.Store.ClaimNextItem(ctx)
	if err != nil {
		return
	}
	if w.Control != nil && !w.Control.IngestAllowsWork() {
		_ = w.Store.RequeueItem(ctx, item.ID)
		return
	}
	w.logItem(ctx, job, item, "start", "начало обработки %s (%s)", item.RegNumber, item.SourceSite)
	if existing, err := w.Store.FindTenderByKey(ctx, item.RegNumber, normalizeSite(item.SourceSite)); err == nil {
		pct := 5
		_ = w.Store.SetTenderProgress(ctx, existing.ID, &pct, nil)
	} else if existing, err := w.Store.FindTenderByKey(ctx, item.RegNumber, "https://zakupki.gov.ru"); err == nil {
		pct := 5
		_ = w.Store.SetTenderProgress(ctx, existing.ID, &pct, nil)
	}

	csvSite := normalizeSite(item.SourceSite)
	if w.Parser == nil || !w.Parser.Enabled() {
		_ = w.Store.FinishItem(ctx, item.ID, "error", "PARSER_URL not configured", nil)
		return
	}
	resp, err := w.Parser.Fetch(ctx, item.RegNumber, csvSite)
	if err != nil {
		w.logItem(ctx, job, item, "error", "parser: %v", err)
		_ = w.Store.FinishItem(ctx, item.ID, "error", err.Error(), nil)
		return
	}
	w.logItem(ctx, job, item, "info", "источник: %s — %s", resp.SourceUsed, resp.Message)

	if resp.Failed || resp.Result == nil {
		msg := resp.Message
		if msg == "" {
			msg = "не удалось проанализировать"
		}
		stubPayload, _ := json.Marshal(map[string]any{
			"csv_source":  csvSite,
			"source_used": resp.SourceUsed,
			"failed":      true,
			"message":     msg,
		})
		tender, _, upErr := w.Store.UpsertTender(ctx, store.TenderUpsertInput{
			RegNumber:  item.RegNumber,
			SourceSite: csvSite,
			Status:     "failed_analyze",
			Payload:    stubPayload,
			CategoryID: job.CategoryID,
		})
		if upErr != nil {
			w.logItem(ctx, job, item, "error", "stub upsert: %v", upErr)
			_ = w.Store.FinishItem(ctx, item.ID, "error", msg, nil)
			return
		}
		tid := tender.ID
		st := "failed_analyze"
		if strings.Contains(msg, "адаптер") || strings.Contains(msg, "не подключён") {
			st = "unsupported_source"
		}
		_ = w.Store.FinishItem(ctx, item.ID, st, msg, &tid)
		w.logItem(ctx, job, item, "done", "завершено со статусом %s", st)
		return
	}

	res := resp.Result
	w.logItem(ctx, job, item, "info", "карточка: law=%s docs=%d object=%q", res.Law, len(res.Documents), truncate(res.ObjectName, 80))

	custIn := store.Customer{
		INN: res.Customer.INN, KPP: res.Customer.KPP, OGRN: res.Customer.OGRN,
		FullName: res.Customer.FullName, Address: res.Customer.Address,
		Email: res.Customer.Email, Phone: res.Customer.Phone, ContactPerson: res.Customer.ContactPerson,
		OrganizationCode: res.Customer.OrganizationCode,
		Payload:          res.Payload,
	}
	if res.Customer223 != nil {
		custIn.INN = res.Customer223.INN
		custIn.KPP = res.Customer223.KPP
		custIn.OGRN = res.Customer223.OGRN
		if res.Customer223.FullName != "" {
			custIn.FullName = res.Customer223.FullName
		}
		custIn.AgencyID = res.Customer223.AgencyID
	}
	cust, err := w.Store.UpsertCustomer(ctx, custIn)
	if err != nil {
		w.logItem(ctx, job, item, "error", "customer: %v", err)
		_ = w.Store.FinishItem(ctx, item.ID, "error", "customer: "+err.Error(), nil)
		return
	}
	cid := cust.ID
	storeSite := "https://zakupki.gov.ru"
	if strings.HasPrefix(resp.SourceUsed, "adapter:") {
		storeSite = csvSite
	}
	payload := injectMeta(res.Payload, csvSite, resp.SourceUsed)
	tender, _, err := w.Store.UpsertTender(ctx, store.TenderUpsertInput{
		RegNumber: res.RegNumber, SourceSite: storeSite, Law: res.Law,
		CustomerID: &cid, ObjectName: res.ObjectName, Status: res.Status,
		NMCK: res.NMCK, Currency: res.Currency,
		PublishedAt: res.PublishedAt, UpdatedOnSite: res.UpdatedOnSite, ApplicationEnd: res.ApplicationEnd,
		Payload: payload, CategoryID: job.CategoryID,
	})
	if err != nil {
		w.logItem(ctx, job, item, "error", "tender: %v", err)
		_ = w.Store.FinishItem(ctx, item.ID, "error", "tender: "+err.Error(), nil)
		return
	}

	startPct := 15
	_ = w.Store.SetTenderProgress(ctx, tender.ID, &startPct, nil)

	var urls []string
	totalDocs := len(res.Documents)
	for i, d := range res.Documents {
		urls = append(urls, d.SourceURL)
		w.logItem(ctx, job, item, "doc", "[%d/%d] %s → %s %s", i+1, totalDocs, d.Filename, d.ProcessStatus, d.ProcessError)
		doc := store.Document{
			TenderID: tender.ID, UID: d.UID, Filename: d.Filename, SourceURL: d.SourceURL,
			GroupTitle: d.GroupTitle, Edition: d.Edition, ProcessStatus: d.ProcessStatus,
			ProcessError: d.ProcessError, ContentHash: d.ContentHash,
		}
		if d.ProcessStatus == "processed" && d.TextContent != "" {
			txt := d.TextContent
			doc.TextContent = &txt
		}
		if _, _, err := w.Store.UpsertDocument(ctx, doc); err != nil {
			w.logItem(ctx, job, item, "warn", "doc upsert: %v", err)
		}
		if totalDocs > 0 {
			pct := 15 + int(float64(i+1)/float64(totalDocs)*80)
			_ = w.Store.SetTenderProgress(ctx, tender.ID, &pct, nil)
		}
	}
	_ = w.Store.MarkMissingDocuments(ctx, tender.ID, urls)
	donePct := 100
	_ = w.Store.SetTenderProgress(ctx, tender.ID, &donePct, nil)
	tid := tender.ID
	_ = w.Store.FinishItem(ctx, item.ID, "ok", "", &tid)
	w.logItem(ctx, job, item, "done", "сохранено в БД, документов: %d", len(res.Documents))
}

func (w *Worker) logItem(ctx context.Context, job *store.IngestJob, item *store.IngestItem, level, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	w.Log.Printf("[%s] %s %s", item.RegNumber, level, msg)
	_ = w.Store.AddJobLog(ctx, job.ID, item.ID, item.RegNumber, level, msg)
}

func injectMeta(payload []byte, csvSite, sourceUsed string) []byte {
	var m map[string]any
	if len(payload) == 0 || string(payload) == "null" {
		m = map[string]any{}
	} else if err := json.Unmarshal(payload, &m); err != nil {
		return payload
	}
	if csvSite != "" {
		m["csv_source_site"] = csvSite
	}
	m["source_used"] = sourceUsed
	b, err := json.Marshal(m)
	if err != nil {
		return payload
	}
	return b
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

func normalizeSite(site string) string {
	site = strings.TrimSpace(site)
	if site == "" {
		return "https://zakupki.gov.ru"
	}
	return strings.TrimRight(site, "/")
}

func ParseCSV(r io.Reader) ([]struct{ Reg, Site string }, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	text := string(data)
	semi := countRune(text, ';')
	comma := countRune(text, ',')
	sep := ';'
	if comma > semi {
		sep = ','
	}
	cr := csv.NewReader(strings.NewReader(text))
	cr.Comma = sep
	cr.LazyQuotes = true
	cr.FieldsPerRecord = -1
	var out []struct{ Reg, Site string }
	first := true
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(rec) == 0 {
			continue
		}
		if first && looksHeader(strings.Join(rec, string(sep))) {
			first = false
			continue
		}
		first = false
		reg, site := parseCSVRow(rec)
		if reg == "" && site == "" {
			continue
		}
		if reg == "" {
			reg = guessRegFromURL(site)
		}
		if site == "" {
			site = "https://zakupki.gov.ru"
		}
		if reg == "" {
			continue
		}
		out = append(out, struct{ Reg, Site string }{reg, site})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty csv")
	}
	return out, nil
}

func parseCSVRow(rec []string) (reg, site string) {
	a := strings.TrimSpace(rec[0])
	b := ""
	if len(rec) > 1 {
		b = strings.TrimSpace(rec[1])
	}
	if looksURL(a) {
		site = a
		if b != "" && !looksURL(b) {
			reg = b
		} else {
			reg = guessRegFromURL(a)
		}
		return reg, site
	}
	reg = a
	if looksURL(b) {
		site = b
	} else if b != "" {
		site = b
	}
	return reg, site
}

func looksURL(s string) bool {
	low := strings.ToLower(s)
	return strings.Contains(low, "://") || strings.HasPrefix(low, "www.")
}

func guessRegFromURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if q := u.Query().Get("regNumber"); q != "" {
		return q
	}
	if q := u.Query().Get("reestrNumber"); q != "" {
		return q
	}
	if q := u.Query().Get("noticeInfoId"); q != "" {
		return q
	}
	base := path.Base(strings.TrimSuffix(u.Path, "/"))
	if base != "" && base != "." && base != "/" {
		return base
	}
	return ""
}

func countRune(s string, r rune) int {
	n := 0
	for _, c := range s {
		if c == r {
			n++
		}
	}
	return n
}

func looksHeader(s string) bool {
	low := strings.ToLower(s)
	return strings.Contains(low, "reg") || strings.Contains(low, "номер") || strings.Contains(low, "site") || strings.Contains(low, "сайт")
}

func StartIngest(ctx context.Context, st *store.Store, categorySlug, categoryTitle, sourceName string, items []struct{ Reg, Site string }) (*store.IngestJob, error) {
	return StartIngestBound(ctx, st, categorySlug, categoryTitle, "", sourceName, items)
}

// StartIngestBound создаёт ingest job, привязанный к категории/списку.
// Приоритет поиска списка: searchConfigID → categorySlug → создание по categoryTitle (+ searchConfigID).
func StartIngestBound(ctx context.Context, st *store.Store, categorySlug, categoryTitle, searchConfigID, sourceName string, items []struct{ Reg, Site string }) (*store.IngestJob, error) {
	cat, err := resolveIngestCategory(ctx, st, categorySlug, categoryTitle, searchConfigID)
	if err != nil {
		return nil, err
	}
	return st.CreateIngestJob(ctx, cat.ID, sourceName, items)
}

func resolveIngestCategory(ctx context.Context, st *store.Store, categorySlug, categoryTitle, searchConfigID string) (*store.Category, error) {
	searchConfigID = strings.TrimSpace(searchConfigID)
	categorySlug = strings.TrimSpace(categorySlug)
	categoryTitle = strings.TrimSpace(categoryTitle)

	if searchConfigID != "" {
		cat, err := st.GetCategoryBySearchConfigID(ctx, searchConfigID)
		if err == nil {
			return cat, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
		// Список для этой конфигурации ещё нет — создаём.
		title := categoryTitle
		if title == "" {
			title = "search-" + searchConfigID
			if len(title) > 64 {
				title = title[:64]
			}
		}
		return st.CreateCategoryWithSearchConfig(ctx, title, categorySlug, searchConfigID)
	}
	if categorySlug != "" {
		return st.GetCategoryBySlug(ctx, categorySlug)
	}
	if categoryTitle != "" {
		return st.CreateCategoryWithSearchConfig(ctx, categoryTitle, "", "")
	}
	return nil, fmt.Errorf("category_slug, category_title or search_config_id required")
}
