package model

import "testing"

func TestParamHashCanonical(t *testing.T) {
	// Key order must not matter (keys sorted, compact separators).
	h1 := ParamHash(map[string]float64{"a": 1, "b": 2})
	h2 := ParamHash(map[string]float64{"b": 2, "a": 1})
	if h1 != h2 {
		t.Fatalf("hash differs for reordered keys: %s vs %s", h1, h2)
	}
	// sha256 hex of canonical JSON {"a":1,"b":2}
	const want = "43258cff783fe7036d8a43033f830adfc60ec037382473548ac742b888292777"
	if h1 != want {
		t.Fatalf("hash = %s, want %s", h1, want)
	}
}

func TestParamHashDistinct(t *testing.T) {
	a := ParamHash(map[string]float64{"lr": 0.01})
	b := ParamHash(map[string]float64{"lr": 0.02})
	if a == b {
		t.Fatalf("different params produced identical hash %s", a)
	}
	if len(a) != 64 {
		t.Fatalf("hash length = %d, want 64 hex chars", len(a))
	}
}

func TestParamHashEmpty(t *testing.T) {
	if ParamHash(nil) != ParamHash(map[string]float64{}) {
		t.Fatal("nil and empty map should hash identically")
	}
}
