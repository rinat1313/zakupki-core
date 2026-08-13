package httpapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSwaggerRoutes(t *testing.T) {
	s := &Server{Mux: http.NewServeMux()}
	s.registerSwagger()
	h := WithCORS(s.Mux)

	t.Run("openapi yaml", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/openapi.yaml", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d", rec.Code)
		}
		body, _ := io.ReadAll(rec.Body)
		if !strings.Contains(string(body), "openapi: 3.0.3") {
			t.Fatalf("missing openapi version in body")
		}
		if !strings.Contains(string(body), "/api/v1/health") {
			t.Fatalf("missing health path")
		}
		ct := rec.Header().Get("Content-Type")
		if !strings.Contains(ct, "yaml") {
			t.Fatalf("unexpected content-type %q", ct)
		}
	})

	t.Run("swagger ui index", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/swagger/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "swagger-ui") {
			t.Fatalf("expected swagger-ui html")
		}
	})

	t.Run("swagger redirect", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/swagger", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusFound {
			t.Fatalf("status=%d", rec.Code)
		}
		if loc := rec.Header().Get("Location"); loc != "/swagger/" {
			t.Fatalf("location=%q", loc)
		}
	})
}
