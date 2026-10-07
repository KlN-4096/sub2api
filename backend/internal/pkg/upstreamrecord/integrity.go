package upstreamrecord

import (
	"encoding/json"
	"errors"
)

const maxExportCaptures = 1000000

type captureIntegrity struct {
	started bool
	ended   bool
	next    uint64
	invalid bool
}

// IntegrityChecker checks the supplied captures, not whether an administrator
// supplied every archive ever produced. Missing starts, frames, ends, duplicate
// segments and source-side loss must never be exported as complete captures.
type IntegrityChecker struct {
	captures map[string]*captureIntegrity
}

func NewIntegrityChecker() *IntegrityChecker {
	return &IntegrityChecker{captures: make(map[string]*captureIntegrity)}
}

func (c *IntegrityChecker) Observe(event Event) (string, error) {
	state := c.captures[event.CaptureID]
	if state == nil {
		if len(c.captures) >= maxExportCaptures {
			return "incomplete", errors.New("export capture limit exceeded")
		}
		state = &captureIntegrity{}
		c.captures[event.CaptureID] = state
	}
	if event.Kind == "capture_start" {
		if state.started || state.ended || state.next != 0 || event.Sequence != 0 {
			state.invalid = true
		}
		state.started, state.next = true, 1
	} else {
		if !state.started || state.ended || event.Sequence != state.next {
			state.invalid = true
		}
		state.next = event.Sequence + 1
	}
	if event.Kind == "capture_end" {
		var summary Summary
		if err := json.Unmarshal(event.Details, &summary); err != nil || !summary.RecordingComplete || summary.LostEvents != 0 {
			state.invalid = true
		}
		state.ended = true
	}
	if state.invalid {
		return "incomplete", nil
	}
	if state.ended {
		return "complete", nil
	}
	return "prefix", nil
}

func (c *IntegrityChecker) Counts() (total, incomplete int) {
	for _, state := range c.captures {
		total++
		if !state.started || !state.ended || state.invalid {
			incomplete++
		}
	}
	return
}

type ExportEvent struct {
	Event
	ExportIntegrity string `json:"export_integrity"`
}
