//go:build gwpoolexperiment

package main

import (
	"strings"
	"testing"
)

func TestQuestionNeedsExplicitCompletion(t *testing.T) {
	answer := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"高市早苗\"}\n\n"
	for _, tc := range []struct {
		name, body string
		complete   bool
	}{
		{"complete", answer + "data: {\"type\":\"response.completed\"}\n\n", true},
		{"eof", answer, false},
		{"fake marker", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"response.completed\"}\n\n", false},
		{"failed", answer + "data: {\"type\":\"response.failed\"}\n\n", false},
		{"limit", strings.Repeat(" ", 1<<20) + "\ndata: {\"type\":\"response.completed\"}\n\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, got := readQuestionAnswer(strings.NewReader(tc.body))
			if got != tc.complete {
				t.Fatalf("complete = %v", got)
			}
		})
	}
}
