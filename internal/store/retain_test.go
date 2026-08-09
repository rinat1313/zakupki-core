package store

import "testing"

func TestNormalizeRetainReason(t *testing.T) {
	cases := map[string]string{
		"":             RetainReasonManual,
		"manual":       RetainReasonManual,
		"interesting":  RetainReasonInteresting,
		"liked":        RetainReasonInteresting,
		"in_work":      RetainReasonInWork,
		"participate":  RetainReasonAIInteresting,
		"caution":      RetainReasonAIInteresting,
		"analyzing":    RetainReasonAnalyzing,
		"custom-tag":   "custom-tag",
	}
	for in, want := range cases {
		if got := NormalizeRetainReason(in); got != want {
			t.Fatalf("%q: got %q want %q", in, got, want)
		}
	}
}
