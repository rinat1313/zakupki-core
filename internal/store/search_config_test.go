package store

import "testing"

func TestSearchConfigSlug(t *testing.T) {
	id := "0fc449d6-4063-4681-b193-458999155279"
	got := searchConfigSlug(id)
	want := "search-0fc449d64063"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNeedAIConfigError(t *testing.T) {
	if ErrNeedAIConfig == nil || ErrNeedAIConfig.Error() == "" {
		t.Fatal("ErrNeedAIConfig must be set")
	}
}

func TestNormalizeRetainReason(t *testing.T) {
	cases := map[string]string{
		"":            RetainReasonManual,
		"interesting": RetainReasonInteresting,
		"in_work":     RetainReasonInWork,
		"participate": RetainReasonAIInteresting,
	}
	for in, want := range cases {
		if got := NormalizeRetainReason(in); got != want {
			t.Fatalf("%q: got %q want %q", in, got, want)
		}
	}
}
