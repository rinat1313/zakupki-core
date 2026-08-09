package ingest

import (
	"strings"
	"testing"
)

func TestResolveIngestRequiresBinding(t *testing.T) {
	_, err := resolveIngestCategory(nil, nil, "", "", "")
	if err == nil {
		t.Fatal("expected error when no binding provided")
	}
	if !strings.Contains(err.Error(), "required") {
		t.Fatalf("unexpected error: %v", err)
	}
}
