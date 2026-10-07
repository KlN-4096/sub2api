package upstreamrecord

import (
	"encoding/json"
	"testing"
)

func TestRecorderExportIntegrityDetectsMissingFramesAndSegments(t *testing.T) {
	details, _ := json.Marshal(Summary{RecordingComplete: true})
	start := Event{CaptureID: "one", Kind: "capture_start", Sequence: 0}
	body := Event{CaptureID: "one", Kind: "response_body", Sequence: 1}
	end := Event{CaptureID: "one", Kind: "capture_end", Sequence: 2, Details: details}
	for name, events := range map[string][]Event{
		"complete":              {start, body, end},
		"missing_start_segment": {body, end},
		"missing_middle_frame":  {start, end},
		"missing_end":           {start, body},
		"duplicate_segment":     {start, body, end, start, body, end},
	} {
		t.Run(name, func(t *testing.T) {
			checker := NewIntegrityChecker()
			last := ""
			for _, event := range events {
				var err error
				last, err = checker.Observe(event)
				if err != nil {
					t.Fatal(err)
				}
			}
			total, incomplete := checker.Counts()
			if total != 1 {
				t.Fatal(total)
			}
			if name == "complete" {
				if last != "complete" || incomplete != 0 {
					t.Fatal(last, incomplete)
				}
			} else if incomplete != 1 || last == "complete" {
				t.Fatalf("incomplete export presented as complete: %s %d", last, incomplete)
			}
		})
	}
}
