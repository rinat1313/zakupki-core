package httpapi

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed openapi.yaml
var openAPISpec []byte

//go:embed swaggerui/*
var swaggerUIFS embed.FS

func (s *Server) registerSwagger() {
	s.Mux.HandleFunc("GET /api/openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(openAPISpec)
	})

	sub, err := fs.Sub(swaggerUIFS, "swaggerui")
	if err != nil {
		panic(err)
	}
	ui := http.StripPrefix("/swagger/", http.FileServer(http.FS(sub)))

	s.Mux.HandleFunc("GET /swagger", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/", http.StatusFound)
	})
	s.Mux.Handle("GET /swagger/", ui)
}
