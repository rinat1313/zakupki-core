package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rinat1313/zakupki-core/internal/store"
)

func TestNewDoesNotPanicOnCategoryRoutes(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("New panicked: %v", r)
		}
	}()
	s := New(nil, nil, nil)
	if s == nil || s.Mux == nil {
		t.Fatal("nil server")
	}
}

func TestMatchCategoryRoute(t *testing.T) {
	cases := []struct {
		method string
		path   string
		kind   categoryRouteKind
		slug   string
		id     string
	}{
		{http.MethodGet, "/api/v1/categories/by-search-config/cfg-1", catRouteGetBySearchID, "", "cfg-1"},
		{http.MethodGet, "/api/v1/categories/by-search-profile/prof-9", catRouteGetBySearchID, "", "prof-9"},
		// historic conflict path: treated as search-config id, not slug/ai-configs
		{http.MethodGet, "/api/v1/categories/by-search-config/ai-configs", catRouteGetBySearchID, "", "ai-configs"},
		{http.MethodGet, "/api/v1/categories/my-slug/ai-configs", catRouteGetAIConfigs, "my-slug", ""},
		{http.MethodPut, "/api/v1/categories/my-slug/auto-ai", catRoutePutAutoAI, "my-slug", ""},
		{http.MethodPut, "/api/v1/categories/by-search-config/cfg-1/auto-ai", catRoutePutSearchAutoAI, "", "cfg-1"},
		{http.MethodPost, "/api/v1/categories/my-slug/sync", catRoutePostSync, "my-slug", ""},
		{http.MethodPost, "/api/v1/categories/by-search-config/cfg-1/sync", catRoutePostSearchSync, "", "cfg-1"},
		{http.MethodPost, "/api/v1/categories/by-search-profile/prof-9/sync", catRoutePostSearchSync, "", "prof-9"},
		{http.MethodGet, "/api/v1/categories/my-slug", catRouteGet, "my-slug", ""},
		{http.MethodPatch, "/api/v1/categories/my-slug", catRoutePatch, "my-slug", ""},
		{http.MethodDelete, "/api/v1/categories/my-slug", catRouteDelete, "my-slug", ""},
		{http.MethodDelete, "/api/v1/categories/my-slug/tenders", catRouteDeleteTenders, "my-slug", ""},
		{http.MethodPut, "/api/v1/categories/my-slug/ai-configs/abc", catRoutePutAIConfig, "my-slug", "abc"},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			rt, ok := matchCategoryRoute(tc.method, tc.path)
			if !ok {
				t.Fatal("no match")
			}
			if rt.kind != tc.kind || rt.slug != tc.slug || rt.id != tc.id {
				t.Fatalf("got kind=%d slug=%q id=%q want kind=%d slug=%q id=%q",
					rt.kind, rt.slug, rt.id, tc.kind, tc.slug, tc.id)
			}
		})
	}
}

func TestCategoriesSubtreeReachesHandlerNotMux404(t *testing.T) {
	s := New(nil, nil, nil)

	// Replace serveCategories with a probe that records PathValues, without hitting Store.
	var gotMethod, gotPath, gotSlug, gotID string
	s.Mux = http.NewServeMux()
	s.Mux.HandleFunc("GET /api/v1/categories", s.listCategories)
	s.Mux.HandleFunc("POST /api/v1/categories", s.createCategory)
	s.Mux.Handle("/api/v1/categories/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rt, ok := matchCategoryRoute(r.Method, r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		gotMethod, gotPath = r.Method, r.URL.Path
		gotSlug, gotID = rt.slug, rt.id
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	s.Mux.HandleFunc("POST /api/v1/search-profiles/{id}/sync", s.syncSearchConfigPool)

	paths := []struct {
		method, path, slug, id string
	}{
		{http.MethodGet, "/api/v1/categories/by-search-config/cfg-1", "", "cfg-1"},
		{http.MethodGet, "/api/v1/categories/my-slug/ai-configs", "my-slug", ""},
		{http.MethodPut, "/api/v1/categories/my-slug/auto-ai", "my-slug", ""},
		{http.MethodPut, "/api/v1/categories/by-search-config/cfg-1/auto-ai", "", "cfg-1"},
		{http.MethodPost, "/api/v1/categories/my-slug/sync", "my-slug", ""},
		{http.MethodPost, "/api/v1/categories/by-search-profile/prof-9/sync", "", "prof-9"},
	}
	for _, tc := range paths {
		gotMethod, gotPath, gotSlug, gotID = "", "", "", ""
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		s.Mux.ServeHTTP(rec, req)
		body, _ := io.ReadAll(rec.Body)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s: status=%d body=%q (mux 404?)", tc.method, tc.path, rec.Code, body)
		}
		if gotMethod != tc.method || gotPath != tc.path {
			t.Fatalf("handler not reached for %s %s", tc.method, tc.path)
		}
		if gotSlug != tc.slug || gotID != tc.id {
			t.Fatalf("%s: slug/id got %q/%q want %q/%q", tc.path, gotSlug, gotID, tc.slug, tc.id)
		}
	}
}

func TestSearchProfilesSyncStillOnMux(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			// sync handler may panic on nil store — that still means mux routed to it.
			_ = r
		}
	}()
	s := New(nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/search-profiles/abc/sync", strings.NewReader(`{"items":[]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.Mux.ServeHTTP(rec, req)
	body, _ := io.ReadAll(rec.Body)
	// Default mux 404 body is plain "404 page not found\n" without JSON error.
	if rec.Code == http.StatusNotFound && string(body) == "404 page not found\n" {
		t.Fatal("search-profiles sync not registered on mux")
	}
}

func TestWriteErrNeedAIConfigIs400(t *testing.T) {
	rec := httptest.NewRecorder()
	writeErr(rec, store.ErrNeedAIConfig)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d want 400", rec.Code)
	}
}
