package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/rinat1313/zakupki-core/internal/analizator"
	"github.com/rinat1313/zakupki-core/internal/autoai"
	"github.com/rinat1313/zakupki-core/internal/control"
	"github.com/rinat1313/zakupki-core/internal/ingest"
	"github.com/rinat1313/zakupki-core/internal/store"

	"github.com/google/uuid"
)

type Server struct {
	Store      *store.Store
	Analizator *analizator.Client
	Control    *control.Controller
	Mux        *http.ServeMux
}

func New(st *store.Store, az *analizator.Client, ctrl *control.Controller) *Server {
	if ctrl == nil {
		ctrl = control.New()
	}
	s := &Server{Store: st, Analizator: az, Control: ctrl, Mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) routes() {
	s.Mux.HandleFunc("GET /api/v1/health", s.health)
	s.Mux.HandleFunc("GET /api/v1/categories", s.listCategories)
	s.Mux.HandleFunc("POST /api/v1/categories", s.createCategory)
	s.Mux.HandleFunc("GET /api/v1/categories/by-search-config/{id}", s.getCategoryBySearchConfig)
	s.Mux.HandleFunc("GET /api/v1/categories/by-search-profile/{id}", s.getCategoryBySearchConfig)
	s.Mux.HandleFunc("GET /api/v1/categories/{slug}", s.getCategory)
	s.Mux.HandleFunc("PATCH /api/v1/categories/{slug}", s.patchCategory)
	s.Mux.HandleFunc("DELETE /api/v1/categories/{slug}", s.deleteCategory)
	s.Mux.HandleFunc("DELETE /api/v1/categories/{slug}/tenders", s.clearCategoryTenders)
	s.Mux.HandleFunc("DELETE /api/v1/categories/{slug}/jobs", s.clearCategoryJobs)
	s.Mux.HandleFunc("POST /api/v1/categories/{slug}/refresh", s.refreshCategory)
	s.Mux.HandleFunc("PUT /api/v1/categories/{slug}/auto-ai", s.setCategoryAutoAI)
	s.Mux.HandleFunc("PUT /api/v1/categories/by-search-config/{id}/auto-ai", s.setSearchConfigAutoAI)
	s.Mux.HandleFunc("PUT /api/v1/categories/by-search-profile/{id}/auto-ai", s.setSearchConfigAutoAI)
	s.Mux.HandleFunc("GET /api/v1/categories/{slug}/ai-configs", s.listAIConfigs)
	s.Mux.HandleFunc("POST /api/v1/categories/{slug}/ai-configs", s.createAIConfig)
	s.Mux.HandleFunc("PUT /api/v1/categories/{slug}/ai-configs/{id}", s.updateAIConfig)
	s.Mux.HandleFunc("DELETE /api/v1/categories/{slug}/ai-configs/{id}", s.deleteAIConfig)
	s.Mux.HandleFunc("PUT /api/v1/categories/{slug}/active-ai-config", s.setActiveAIConfig)

	s.Mux.HandleFunc("POST /api/v1/ingest", s.ingest)
	s.Mux.HandleFunc("POST /api/v1/ingest/items", s.ingestItems)
	s.Mux.HandleFunc("GET /api/v1/ingest/jobs", s.listJobs)
	s.Mux.HandleFunc("GET /api/v1/ingest/jobs/{id}", s.getJob)
	s.Mux.HandleFunc("GET /api/v1/ingest/jobs/{id}/logs", s.jobLogs)
	s.Mux.HandleFunc("DELETE /api/v1/ingest/jobs/{id}", s.deleteJob)
	s.Mux.HandleFunc("GET /api/v1/stats/ingest", s.ingestStats)

	s.Mux.HandleFunc("GET /api/v1/tenders", s.listTenders)
	s.Mux.HandleFunc("GET /api/v1/tenders/{id}", s.getTender)
	s.Mux.HandleFunc("PATCH /api/v1/tenders/{id}", s.patchTender)
	s.Mux.HandleFunc("DELETE /api/v1/tenders/{id}", s.deleteTender)
	s.Mux.HandleFunc("POST /api/v1/tenders/{id}/retain", s.retainTender)
	s.Mux.HandleFunc("DELETE /api/v1/tenders/{id}/retain", s.unretainTender)
	s.Mux.HandleFunc("POST /api/v1/tenders/{id}/refresh", s.refreshTender)
	s.Mux.HandleFunc("POST /api/v1/categories/{slug}/sync", s.syncCategoryPool)
	s.Mux.HandleFunc("POST /api/v1/categories/by-search-config/{id}/sync", s.syncSearchConfigPool)
	s.Mux.HandleFunc("POST /api/v1/categories/by-search-profile/{id}/sync", s.syncSearchConfigPool)
	s.Mux.HandleFunc("POST /api/v1/search-profiles/{id}/sync", s.syncSearchConfigPool)
	s.Mux.HandleFunc("GET /api/v1/tenders/{id}/documents", s.listDocuments)
	s.Mux.HandleFunc("GET /api/v1/tenders/{id}/events", s.listEvents)
	s.Mux.HandleFunc("GET /api/v1/tenders/{id}/assessment", s.getAssessment)
	s.Mux.HandleFunc("PUT /api/v1/tenders/{id}/assessment", s.putAssessment)
	s.Mux.HandleFunc("POST /api/v1/tenders/{id}/analyze", s.analyzeTender)

	s.Mux.HandleFunc("GET /api/v1/workers", s.workersStatus)
	s.Mux.HandleFunc("PUT /api/v1/workers/auto-ai", s.setAutoAI)
	s.Mux.HandleFunc("POST /api/v1/workers/ingest/pause", s.ingestPause)
	s.Mux.HandleFunc("POST /api/v1/workers/ingest/resume", s.ingestResume)
	s.Mux.HandleFunc("POST /api/v1/workers/ingest/stop", s.ingestStop)
	s.Mux.HandleFunc("POST /api/v1/workers/analyze/stop", s.analyzeStop)

	s.Mux.HandleFunc("GET /api/v1/customers", s.listCustomers)
	s.Mux.HandleFunc("GET /api/v1/customers/{id}", s.getCustomer)
	s.Mux.HandleFunc("POST /api/v1/customers", s.createCustomer)
	s.Mux.HandleFunc("PATCH /api/v1/customers/{id}", s.patchCustomer)
	s.Mux.HandleFunc("DELETE /api/v1/customers/{id}", s.deleteCustomer)

	s.Mux.HandleFunc("GET /api/v1/customers/{id}/courts", s.stubList)
	s.Mux.HandleFunc("GET /api/v1/customers/{id}/rnp", s.stubList)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	out := map[string]any{"status": "ok"}
	if s.Analizator != nil && s.Analizator.Enabled() {
		out["analizator_url"] = s.Analizator.BaseURL
		if err := s.Analizator.Ping(r.Context()); err != nil {
			out["analizator"] = "unavailable"
			out["analizator_error"] = err.Error()
		} else {
			out["analizator"] = "ok"
		}
	} else {
		out["analizator"] = "disabled"
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listCategories(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.ListCategories(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(list))
}

func (s *Server) createCategory(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title           string `json:"title"`
		Slug            string `json:"slug"`
		SearchConfigID  string `json:"search_config_id"`
		SearchProfileID string `json:"search_profile_id"`
		AutoAI          *bool  `json:"auto_ai"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	sid := strings.TrimSpace(body.SearchConfigID)
	if sid == "" {
		sid = strings.TrimSpace(body.SearchProfileID)
	}
	c, err := s.Store.CreateCategoryWithSearchConfig(r.Context(), body.Title, body.Slug, sid)
	if err != nil {
		writeErr(w, err)
		return
	}
	if body.AutoAI != nil {
		c, err = s.Store.SetCategoryAutoAI(r.Context(), c.ID, *body.AutoAI)
		if err != nil {
			writeErr(w, err)
			return
		}
	}
	writeJSON(w, http.StatusCreated, c)
}

func (s *Server) getCategory(w http.ResponseWriter, r *http.Request) {
	c, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) getCategoryBySearchConfig(w http.ResponseWriter, r *http.Request) {
	c, err := s.Store.GetCategoryBySearchConfigID(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) patchCategory(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title           *string `json:"title"`
		Slug            *string `json:"slug"`
		SearchConfigID  *string `json:"search_config_id"`
		SearchProfileID *string `json:"search_profile_id"`
		AutoAI          *bool   `json:"auto_ai"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	sid := body.SearchConfigID
	if sid == nil {
		sid = body.SearchProfileID
	}
	c, err := s.Store.UpdateCategory(r.Context(), r.PathValue("slug"), body.Title, body.Slug, sid)
	if err != nil {
		writeErr(w, err)
		return
	}
	if body.AutoAI != nil {
		c, err = s.Store.SetCategoryAutoAI(r.Context(), c.ID, *body.AutoAI)
		if err != nil {
			writeErr(w, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) setCategoryAutoAI(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	s.writeCategoryAutoAI(w, r, cat)
}

func (s *Server) setSearchConfigAutoAI(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySearchConfigID(r.Context(), r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	s.writeCategoryAutoAI(w, r, cat)
}

func (s *Server) writeCategoryAutoAI(w http.ResponseWriter, r *http.Request, cat *store.Category) {
	var body struct {
		Enabled *bool `json:"enabled"`
		AutoAI  *bool `json:"auto_ai"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	on := false
	switch {
	case body.Enabled != nil:
		on = *body.Enabled
	case body.AutoAI != nil:
		on = *body.AutoAI
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "нужно поле enabled или auto_ai"})
		return
	}
	if on && (s.Analizator == nil || !s.Analizator.Enabled()) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "AI-анализатор не настроен (ANALIZATOR_URL)",
		})
		return
	}
	// Включаем ingest — сбор документов нужен до AI.
	if on {
		s.Control.ResumeIngest()
	}
	out, err := s.Store.SetCategoryAutoAI(r.Context(), cat.ID, on)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteCategory(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.DeleteCategory(r.Context(), r.PathValue("slug")); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) ingest(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeErr(w, err)
		return
	}
	slug := strings.TrimSpace(r.FormValue("category_slug"))
	title := strings.TrimSpace(r.FormValue("category_title"))
	searchConfigID := strings.TrimSpace(r.FormValue("search_config_id"))
	if slug == "" && title == "" && searchConfigID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "category_slug, category_title or search_config_id required"})
		return
	}
	file, hdr, err := r.FormFile("file")
	if err != nil {
		writeErr(w, err)
		return
	}
	defer file.Close()
	items, err := ingest.ParseCSV(file)
	if err != nil {
		writeErr(w, err)
		return
	}
	name := ""
	if hdr != nil {
		name = hdr.Filename
	}
	// New upload resumes collection if it was stopped/paused.
	s.Control.ResumeIngest()
	job, err := ingest.StartIngestBound(r.Context(), s.Store, slug, title, searchConfigID, name, items)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

// ingestItems — программная постановка в очередь (от поисковика) без CSV.
// Привязка списка: search_config_id и/или category_slug / category_title.
func (s *Server) ingestItems(w http.ResponseWriter, r *http.Request) {
	var body struct {
		SearchConfigID  string `json:"search_config_id"`
		SearchProfileID string `json:"search_profile_id"`
		CategorySlug    string `json:"category_slug"`
		CategoryTitle   string `json:"category_title"`
		SourceName      string `json:"source_name"`
		Items           []struct {
			RegNumber  string `json:"reg_number"`
			SourceSite string `json:"source_site"`
			NoticeURL  string `json:"notice_url"`
		} `json:"items"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	sid := strings.TrimSpace(body.SearchConfigID)
	if sid == "" {
		sid = strings.TrimSpace(body.SearchProfileID)
	}
	if sid == "" && strings.TrimSpace(body.CategorySlug) == "" && strings.TrimSpace(body.CategoryTitle) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "search_config_id/search_profile_id, category_slug or category_title required"})
		return
	}
	if len(body.Items) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "items required"})
		return
	}
	items := make([]struct{ Reg, Site string }, 0, len(body.Items))
	for _, it := range body.Items {
		reg := strings.TrimSpace(it.RegNumber)
		site := strings.TrimSpace(it.SourceSite)
		if site == "" {
			site = strings.TrimSpace(it.NoticeURL)
		}
		if reg == "" && site == "" {
			continue
		}
		items = append(items, struct{ Reg, Site string }{Reg: reg, Site: site})
	}
	if len(items) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "no valid items"})
		return
	}
	src := strings.TrimSpace(body.SourceName)
	if src == "" {
		src = "searcher"
	}
	s.Control.ResumeIngest()
	job, err := ingest.StartIngestBound(r.Context(), s.Store, body.CategorySlug, body.CategoryTitle, sid, src, items)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.ListJobs(r.Context(), 100)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(list))
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	job, items, err := s.Store.GetJob(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "items": emptySlice(items)})
}

func (s *Server) jobLogs(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	after := int64(0)
	if v := r.URL.Query().Get("after"); v != "" {
		fmt.Sscanf(v, "%d", &after)
	}
	logs, err := s.Store.ListJobLogs(r.Context(), id, after, 300)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(logs))
}

func (s *Server) deleteJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := s.Store.DeleteJob(r.Context(), id); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) clearCategoryTenders(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	n, err := s.Store.ClearCategoryTenders(r.Context(), cat.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}

func (s *Server) clearCategoryJobs(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	n, err := s.Store.DeleteJobsByCategory(r.Context(), cat.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}

func (s *Server) refreshCategory(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		Statuses  []string    `json:"statuses"`
		TenderIDs []uuid.UUID `json:"tender_ids"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	job, err := s.Store.EnqueueRefresh(r.Context(), cat.ID, body.Statuses, body.TenderIDs)
	if err != nil {
		writeErr(w, err)
		return
	}
	s.Control.ResumeIngest()
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) refreshTender(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	t, err := s.Store.GetTender(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	var categoryID uuid.UUID
	if len(t.CategorySlugs) > 0 {
		if c, err := s.Store.GetCategoryBySlug(r.Context(), t.CategorySlugs[0]); err == nil {
			categoryID = c.ID
		}
	}
	if categoryID == uuid.Nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "tender has no category"})
		return
	}
	job, err := s.Store.EnqueueRefresh(r.Context(), categoryID, nil, []uuid.UUID{id})
	if err != nil {
		writeErr(w, err)
		return
	}
	s.Control.ResumeIngest()
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) ingestStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.Store.IngestStats(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(stats))
}

func (s *Server) listTenders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	retained := q.Get("retained") == "1" || q.Get("retained") == "true" || q.Get("workspace") == "1"
	searchID := q.Get("search_config_id")
	if searchID == "" {
		searchID = q.Get("search_profile_id")
	}
	list, err := s.Store.ListTendersFiltered(r.Context(), store.ListTendersFilter{
		CategorySlug:   q.Get("category"),
		SearchConfigID: searchID,
		Q:              q.Get("q"),
		Status:         q.Get("status"),
		RetainedOnly:   retained,
		Limit:          500,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(list))
}

func (s *Server) getTender(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	t, err := s.Store.GetTender(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	_ = s.Store.EnrichTenderUI(r.Context(), t)
	out := map[string]any{}
	b, _ := json.Marshal(t)
	_ = json.Unmarshal(b, &out)
	if t.CustomerID != nil {
		if c, err := s.Store.GetCustomer(r.Context(), *t.CustomerID); err == nil {
			out["customer"] = c
		}
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) patchTender(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var patch map[string]any
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeErr(w, err)
		return
	}
	t, err := s.Store.UpdateTender(r.Context(), id, patch)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) deleteTender(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := s.Store.DeleteTender(r.Context(), id); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) retainTender(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		Reason string `json:"reason"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	t, err := s.Store.RetainTender(r.Context(), id, body.Reason)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) unretainTender(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	t, err := s.Store.UnretainTender(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

func (s *Server) syncCategoryPool(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	s.syncPool(w, r, cat)
}

func (s *Server) syncSearchConfigPool(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.PathValue("id"))
	cat, err := s.Store.GetCategoryBySearchConfigID(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		// Первый sync от zakupki-search — создаём список под profile id.
		title := "search-" + id
		if len(title) > 64 {
			title = title[:64]
		}
		cat, err = s.Store.CreateCategoryWithSearchConfig(r.Context(), title, "", id)
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	s.syncPool(w, r, cat)
}

func (s *Server) syncPool(w http.ResponseWriter, r *http.Request, cat *store.Category) {
	var body struct {
		SearchProfileID string `json:"search_profile_id"`
		SearchConfigID  string `json:"search_config_id"`
		ConfigVersion   int64  `json:"config_version"`
		Items           []struct {
			RegNumber  string `json:"reg_number"`
			SourceSite string `json:"source_site"`
			NoticeURL  string `json:"notice_url"`
			Law        string `json:"law"`
		} `json:"items"`
		Enqueue    *bool  `json:"enqueue"`
		SourceName string `json:"source_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	// Если в теле другой profile id — перепроверим / привяжем.
	wantID := strings.TrimSpace(body.SearchProfileID)
	if wantID == "" {
		wantID = strings.TrimSpace(body.SearchConfigID)
	}
	if wantID != "" && (cat.SearchConfigID == nil || *cat.SearchConfigID != wantID) {
		updated, err := s.Store.UpdateCategory(r.Context(), cat.Slug, nil, nil, &wantID)
		if err != nil {
			writeErr(w, err)
			return
		}
		cat = updated
	}
	items := make([]struct{ Reg, Site string }, 0, len(body.Items))
	for _, it := range body.Items {
		site := strings.TrimSpace(it.SourceSite)
		if site == "" && strings.TrimSpace(it.NoticeURL) != "" {
			site = strings.TrimSpace(it.NoticeURL)
		}
		items = append(items, struct{ Reg, Site string }{Reg: it.RegNumber, Site: site})
	}
	enqueue := true
	if body.Enqueue != nil {
		enqueue = *body.Enqueue
	}
	s.Control.ResumeIngest()
	res, err := s.Store.SyncSearchPoolOpts(r.Context(), cat.ID, store.SyncSearchPoolOpts{
		Items: items, Enqueue: enqueue, SourceName: body.SourceName, ConfigVersion: body.ConfigVersion,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	fresh, _ := s.Store.GetCategoryBySlug(r.Context(), cat.Slug)
	if fresh != nil {
		cat = fresh
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"category": cat,
		"sync":     res,
	})
}

func (s *Server) listDocuments(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	includeText := r.URL.Query().Get("text") == "1" || r.URL.Query().Get("text") == "true"
	list, err := s.Store.ListDocuments(r.Context(), id, includeText)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(list))
}

func (s *Server) listEvents(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	list, err := s.Store.ListEvents(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(list))
}

func (s *Server) getAssessment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	a, err := s.Store.GetAssessment(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeJSON(w, http.StatusOK, map[string]any{"tender_id": id, "summary": "", "details": map[string]any{}})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) putAssessment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body store.Assessment
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	body.TenderID = id
	a, err := s.Store.UpsertAssessment(r.Context(), body)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) analyzeTender(w http.ResponseWriter, r *http.Request) {
	if s.Analizator == nil || !s.Analizator.Enabled() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "analizator выключен: задайте ANALIZATOR_URL",
		})
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		ChecklistID string `json:"checklist_id"`
		ConfigID    string `json:"config_id"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)

	t, err := s.Store.GetTender(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}

	log.Printf("analyze tender %s reg=%s → %s", id, t.RegNumber, s.Analizator.BaseURL)
	opt := &autoai.AnalyzeOptions{ChecklistID: body.ChecklistID, ConfigID: body.ConfigID}
	if err := autoai.AnalyzeTender(r.Context(), s.Store, s.Control, s.Analizator, t, opt); err != nil {
		if errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "canceled") || strings.Contains(err.Error(), "cancelled") {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "AI-анализ остановлен"})
			return
		}
		if strings.Contains(err.Error(), "нет текста") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	a, err := s.Store.GetAssessment(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"assessment": a,
	})
}

func (s *Server) workersStatus(w http.ResponseWriter, r *http.Request) {
	out := s.enrichedWorkersStatus(r.Context())
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) enrichedWorkersStatus(ctx context.Context) map[string]any {
	out := s.Control.Status()
	azOK := s.Analizator != nil && s.Analizator.Enabled()
	out["analizator_configured"] = azOK
	if azOK {
		out["analizator"] = "ok"
	} else {
		out["analizator"] = "disabled"
	}
	catOn, _ := s.Store.AnyCategoryAutoAI(ctx)
	out["category_auto_ai"] = catOn
	// Для UI: AI «включён», если глобально или хотя бы у одного поисковика.
	if catOn {
		out["auto_ai_effective"] = true
	} else {
		out["auto_ai_effective"] = out["auto_ai"]
	}
	return out
}

func (s *Server) setAutoAI(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool `json:"enabled"`
		AutoAI  *bool `json:"auto_ai"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	on := false
	switch {
	case body.Enabled != nil:
		on = *body.Enabled
	case body.AutoAI != nil:
		on = *body.AutoAI
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "нужно поле enabled или auto_ai"})
		return
	}
	if on && (s.Analizator == nil || !s.Analizator.Enabled()) {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "AI-анализатор не настроен (ANALIZATOR_URL)",
		})
		return
	}
	s.Control.SetAutoAI(on)
	if on {
		s.Control.ResumeIngest()
	} else {
		// Не гасим текущие AI, если у поисковиков ещё включён свой auto_ai.
		if catOn, _ := s.Store.AnyCategoryAutoAI(r.Context()); !catOn {
			s.Control.StopAnalyze()
		}
	}
	writeJSON(w, http.StatusOK, s.enrichedWorkersStatus(r.Context()))
}

func (s *Server) ingestPause(w http.ResponseWriter, r *http.Request) {
	s.Control.PauseIngest()
	writeJSON(w, http.StatusOK, s.Control.Status())
}

func (s *Server) ingestResume(w http.ResponseWriter, r *http.Request) {
	s.Control.ResumeIngest()
	writeJSON(w, http.StatusOK, s.Control.Status())
}

func (s *Server) ingestStop(w http.ResponseWriter, r *http.Request) {
	s.Control.StopIngest()
	items, jobs, err := s.Store.CancelActiveIngest(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	out := s.Control.Status()
	out["cancelled_items"] = items
	out["cancelled_jobs"] = jobs
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) analyzeStop(w http.ResponseWriter, r *http.Request) {
	s.Control.StopAnalyze()
	writeJSON(w, http.StatusOK, s.Control.Status())
}

func (s *Server) listCustomers(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.ListCustomers(r.Context(), r.URL.Query().Get("q"), 200)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, emptySlice(list))
}

func (s *Server) getCustomer(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	c, err := s.Store.GetCustomer(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

func (s *Server) createCustomer(w http.ResponseWriter, r *http.Request) {
	var c store.Customer
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeErr(w, err)
		return
	}
	out, err := s.Store.UpsertCustomer(r.Context(), c)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (s *Server) patchCustomer(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var c store.Customer
	if err := json.NewDecoder(r.Body).Decode(&c); err != nil {
		writeErr(w, err)
		return
	}
	out, err := s.Store.UpdateCustomer(r.Context(), id, c)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) deleteCustomer(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	if err := s.Store.DeleteCustomer(r.Context(), id); err != nil {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) stubList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, []any{})
}

func (s *Server) listAIConfigs(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	list, err := s.Store.ListAIConfigs(r.Context(), cat.ID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"configs":             emptySlice(list),
		"active_ai_config_id": cat.ActiveAIConfigID,
	})
}

func (s *Server) createAIConfig(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		Name         string `json:"name"`
		SystemPrompt string `json:"system_prompt"`
		UserPrompt   string `json:"user_prompt"`
		Rules        string `json:"rules"`
		Activate     bool   `json:"activate"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	cfg, err := s.Store.CreateAIConfig(r.Context(), cat.ID, body.Name, body.SystemPrompt, body.UserPrompt, body.Rules)
	if err != nil {
		writeErr(w, err)
		return
	}
	wantActive := body.Activate || cat.ActiveAIConfigID == nil
	if wantActive {
		if s.Control.AutoAIEnabled() && cat.ActiveAIConfigID != nil {
			// при Авто не меняем активную; новая конфиг просто сохраняется
		} else {
			_ = s.Store.SetCategoryActiveAIConfig(r.Context(), cat.ID, &cfg.ID)
		}
	}
	writeJSON(w, http.StatusCreated, cfg)
}

func (s *Server) updateAIConfig(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	cur, err := s.Store.GetAIConfig(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	if cur.CategoryID != cat.ID {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "конфигурация другой категории"})
		return
	}
	var body struct {
		Name         string `json:"name"`
		SystemPrompt string `json:"system_prompt"`
		UserPrompt   string `json:"user_prompt"`
		Rules        string `json:"rules"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	cfg, err := s.Store.UpdateAIConfig(r.Context(), id, body.Name, body.SystemPrompt, body.UserPrompt, body.Rules)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, cfg)
}

func (s *Server) deleteAIConfig(w http.ResponseWriter, r *http.Request) {
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	cur, err := s.Store.GetAIConfig(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	if cur.CategoryID != cat.ID {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "конфигурация другой категории"})
		return
	}
	if err := s.Store.DeleteAIConfig(r.Context(), id); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) setActiveAIConfig(w http.ResponseWriter, r *http.Request) {
	if s.Control.AutoAIEnabled() {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "нельзя менять конфигурацию при включённом Авто AI",
		})
		return
	}
	cat, err := s.Store.GetCategoryBySlug(r.Context(), r.PathValue("slug"))
	if err != nil {
		writeErr(w, err)
		return
	}
	var body struct {
		ConfigID *string `json:"config_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, err)
		return
	}
	var idPtr *uuid.UUID
	if body.ConfigID != nil && strings.TrimSpace(*body.ConfigID) != "" {
		id, err := uuid.Parse(strings.TrimSpace(*body.ConfigID))
		if err != nil {
			writeErr(w, err)
			return
		}
		idPtr = &id
	}
	if err := s.Store.SetCategoryActiveAIConfig(r.Context(), cat.ID, idPtr); err != nil {
		writeErr(w, err)
		return
	}
	updated, _ := s.Store.GetCategoryBySlug(r.Context(), cat.Slug)
	writeJSON(w, http.StatusOK, updated)
}

func emptySlice[T any](v []T) []T {
	if v == nil {
		return []T{}
	}
	return v
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, store.ErrNotFound) {
		code = http.StatusNotFound
	}
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func WithCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
