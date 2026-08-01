package staleref

import (
	"strings"
	"testing"
)

func TestRefusalErrorMentionsRecovery(t *testing.T) {
	err := RefusalError()
	if err == nil {
		t.Fatal("RefusalError() returned nil, want a non-nil error")
	}

	for _, want := range []string{
		"stale",
		"external-name",
		"finalizer",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("RefusalError() = %q, want it to mention %q", err.Error(), want)
		}
	}
}

func TestObserveRefusalErrorMentionsRecovery(t *testing.T) {
	err := ObserveRefusalError()
	if err == nil {
		t.Fatal("ObserveRefusalError() returned nil, want a non-nil error")
	}

	for _, want := range []string{
		"stale",
		"external-name",
		"finalizer",
		"ownership",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ObserveRefusalError() = %q, want it to mention %q", err.Error(), want)
		}
	}
}
