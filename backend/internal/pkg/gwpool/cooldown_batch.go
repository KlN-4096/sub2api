package gwpool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

type CooldownReportResult struct {
	ID             string                  `json:"id"`
	Status         int                     `json:"status"`
	Recommendation *CooldownRecommendation `json:"recommendation,omitempty"`
}

// ReportCooldownBatch retains partial acknowledgements. Unknown old endpoints
// fall back to the original single-report protocol with the same event IDs.
func (c *Client) ReportCooldownBatch(ctx context.Context, reports []CooldownReport) ([]CooldownReportResult, error) {
	if c == nil || len(reports) == 0 || len(reports) > CooldownBatchLimit {
		return nil, fmt.Errorf("%w: invalid cooldown batch size", ErrPool)
	}
	ids := map[string]struct{}{}
	for _, report := range reports {
		if _, duplicate := ids[report.ID]; duplicate || !validCooldownReport(report) {
			return nil, fmt.Errorf("%w: invalid cooldown batch item", ErrPool)
		}
		ids[report.ID] = struct{}{}
	}
	raw, err := json.Marshal(struct {
		Reports []CooldownReport `json:"reports"`
	}{reports})
	if err != nil {
		return nil, fmt.Errorf("%w: encode cooldown batch", ErrPool)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint("cooldown/reports"), bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: build cooldown batch", ErrPool)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(cooldownMaxHeader, strconv.Itoa(CooldownMaxSeconds))
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		_ = resp.Body.Close()
		results := make([]CooldownReportResult, 0, len(reports))
		for _, report := range reports {
			rec, sendErr := c.ReportCooldown(ctx, report)
			result := CooldownReportResult{ID: report.ID, Status: http.StatusOK, Recommendation: rec}
			if sendErr != nil {
				result.Status = http.StatusServiceUnavailable
				var refused *PoolError
				if errors.As(sendErr, &refused) && refused.Status >= http.StatusBadRequest {
					result.Status = refused.Status
				}
			}
			results = append(results, result)
		}
		return results, nil
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &PoolError{Status: resp.StatusCode}
	}
	var out struct {
		OK      bool                   `json:"ok"`
		Results []CooldownReportResult `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxResponseBytes)).Decode(&out); err != nil || !out.OK || len(out.Results) != len(reports) {
		return nil, fmt.Errorf("%w: invalid cooldown batch acknowledgement", ErrPool)
	}
	for i := range out.Results {
		result := &out.Results[i]
		if _, exists := ids[result.ID]; !exists || (result.Status != http.StatusOK && (result.Status < 400 || result.Status > 599)) {
			return nil, fmt.Errorf("%w: invalid cooldown item acknowledgement", ErrPool)
		}
		delete(ids, result.ID)
		if result.Recommendation != nil && !result.Recommendation.Valid() {
			result.Recommendation = nil
		}
	}
	return out.Results, nil
}
