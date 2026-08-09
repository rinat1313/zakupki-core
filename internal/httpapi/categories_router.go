package httpapi

import (
	"net/http"
	"strings"
)

type categoryRouteKind int

const (
	catRouteNone categoryRouteKind = iota
	catRouteGetBySearchID
	catRoutePutSearchAutoAI
	catRoutePostSearchSync
	catRouteGet
	catRoutePatch
	catRouteDelete
	catRouteDeleteTenders
	catRouteDeleteJobs
	catRoutePostRefresh
	catRoutePutAutoAI
	catRoutePostSync
	catRoutePutActiveAIConfig
	catRouteGetAIConfigs
	catRoutePostAIConfigs
	catRoutePutAIConfig
	catRouteDeleteAIConfig
)

type categoryRoute struct {
	kind categoryRouteKind
	slug string
	id   string
}

// matchCategoryRoute разбирает /api/v1/categories/... без ServeMux wildcards.
func matchCategoryRoute(method, path string) (categoryRoute, bool) {
	const prefix = "/api/v1/categories/"
	if !strings.HasPrefix(path, prefix) {
		return categoryRoute{}, false
	}
	rest := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if rest == "" {
		return categoryRoute{}, false
	}
	parts := strings.Split(rest, "/")

	if len(parts) >= 2 && (parts[0] == "by-search-config" || parts[0] == "by-search-profile") {
		id := parts[1]
		switch {
		case len(parts) == 2 && method == http.MethodGet:
			return categoryRoute{kind: catRouteGetBySearchID, id: id}, true
		case len(parts) == 3 && parts[2] == "auto-ai" && method == http.MethodPut:
			return categoryRoute{kind: catRoutePutSearchAutoAI, id: id}, true
		case len(parts) == 3 && parts[2] == "sync" && method == http.MethodPost:
			return categoryRoute{kind: catRoutePostSearchSync, id: id}, true
		default:
			return categoryRoute{}, false
		}
	}

	slug := parts[0]
	switch {
	case len(parts) == 1 && method == http.MethodGet:
		return categoryRoute{kind: catRouteGet, slug: slug}, true
	case len(parts) == 1 && method == http.MethodPatch:
		return categoryRoute{kind: catRoutePatch, slug: slug}, true
	case len(parts) == 1 && method == http.MethodDelete:
		return categoryRoute{kind: catRouteDelete, slug: slug}, true
	case len(parts) == 2 && parts[1] == "tenders" && method == http.MethodDelete:
		return categoryRoute{kind: catRouteDeleteTenders, slug: slug}, true
	case len(parts) == 2 && parts[1] == "jobs" && method == http.MethodDelete:
		return categoryRoute{kind: catRouteDeleteJobs, slug: slug}, true
	case len(parts) == 2 && parts[1] == "refresh" && method == http.MethodPost:
		return categoryRoute{kind: catRoutePostRefresh, slug: slug}, true
	case len(parts) == 2 && parts[1] == "auto-ai" && method == http.MethodPut:
		return categoryRoute{kind: catRoutePutAutoAI, slug: slug}, true
	case len(parts) == 2 && parts[1] == "sync" && method == http.MethodPost:
		return categoryRoute{kind: catRoutePostSync, slug: slug}, true
	case len(parts) == 2 && parts[1] == "active-ai-config" && method == http.MethodPut:
		return categoryRoute{kind: catRoutePutActiveAIConfig, slug: slug}, true
	case len(parts) == 2 && parts[1] == "ai-configs" && method == http.MethodGet:
		return categoryRoute{kind: catRouteGetAIConfigs, slug: slug}, true
	case len(parts) == 2 && parts[1] == "ai-configs" && method == http.MethodPost:
		return categoryRoute{kind: catRoutePostAIConfigs, slug: slug}, true
	case len(parts) == 3 && parts[1] == "ai-configs" && method == http.MethodPut:
		return categoryRoute{kind: catRoutePutAIConfig, slug: slug, id: parts[2]}, true
	case len(parts) == 3 && parts[1] == "ai-configs" && method == http.MethodDelete:
		return categoryRoute{kind: catRouteDeleteAIConfig, slug: slug, id: parts[2]}, true
	default:
		return categoryRoute{}, false
	}
}

// serveCategories разбирает /api/v1/categories/... вручную.
// Нужен из‑за конфликта Go 1.22+ ServeMux между
// /categories/{slug}/... и /categories/by-search-config/{id}.
func (s *Server) serveCategories(w http.ResponseWriter, r *http.Request) {
	rt, ok := matchCategoryRoute(r.Method, r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if rt.slug != "" {
		r.SetPathValue("slug", rt.slug)
	}
	if rt.id != "" {
		r.SetPathValue("id", rt.id)
	}
	switch rt.kind {
	case catRouteGetBySearchID:
		s.getCategoryBySearchConfig(w, r)
	case catRoutePutSearchAutoAI:
		s.setSearchConfigAutoAI(w, r)
	case catRoutePostSearchSync:
		s.syncSearchConfigPool(w, r)
	case catRouteGet:
		s.getCategory(w, r)
	case catRoutePatch:
		s.patchCategory(w, r)
	case catRouteDelete:
		s.deleteCategory(w, r)
	case catRouteDeleteTenders:
		s.clearCategoryTenders(w, r)
	case catRouteDeleteJobs:
		s.clearCategoryJobs(w, r)
	case catRoutePostRefresh:
		s.refreshCategory(w, r)
	case catRoutePutAutoAI:
		s.setCategoryAutoAI(w, r)
	case catRoutePostSync:
		s.syncCategoryPool(w, r)
	case catRoutePutActiveAIConfig:
		s.setActiveAIConfig(w, r)
	case catRouteGetAIConfigs:
		s.listAIConfigs(w, r)
	case catRoutePostAIConfigs:
		s.createAIConfig(w, r)
	case catRoutePutAIConfig:
		s.updateAIConfig(w, r)
	case catRouteDeleteAIConfig:
		s.deleteAIConfig(w, r)
	default:
		http.NotFound(w, r)
	}
}
