package gwpool

import (
	"bytes"
	"context"
	"encoding/json"
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

func (r CooldownReportResult) acknowledged() (string, int) { return r.ID, r.Status }

// ReportCooldownBatch sends cooldown reports of any members of this key at once
// and keeps each report's own acknowledgement and recommendation.
func (c *Client) ReportCooldownBatch(ctx context.Context, reports []CooldownReport) ([]CooldownReportResult, error) {
	results, err := postReportBatch[CooldownReport, CooldownReportResult](ctx, c, "cooldown", reports, CooldownBatchLimit,
		http.Header{cooldownMaxHeader: {strconv.Itoa(CooldownMaxSeconds)}})
	for i := range results {
		if results[i].Recommendation != nil && !results[i].Recommendation.Valid() {
			results[i].Recommendation = nil
		}
	}
	return results, err
}

type batchReport interface {
	Valid() bool
	reportID() string
}

type batchAcknowledgement interface {
	acknowledged() (id string, status int)
}

// postReportBatch posts {"reports": reports} to <kind>/reports and requires
// exactly one valid acknowledgement per report. An unsupported batch endpoint
// is a protocol error, not permission to send single legacy requests.
func postReportBatch[T batchReport, R batchAcknowledgement](ctx context.Context, c *Client, kind string,
	reports []T, limit int, header http.Header) ([]R, error) {
	if c == nil || len(reports) == 0 || len(reports) > limit {
		return nil, fmt.Errorf("%w: invalid %s batch size", ErrPool, kind)
	}
	ids := map[string]struct{}{}
	for _, report := range reports {
		if _, duplicate := ids[report.reportID()]; duplicate || !report.Valid() {
			return nil, fmt.Errorf("%w: invalid %s batch item", ErrPool, kind)
		}
		ids[report.reportID()] = struct{}{}
	}
	raw, err := json.Marshal(struct {
		Reports []T `json:"reports"`
	}{reports})
	if err != nil {
		return nil, fmt.Errorf("%w: encode %s batch", ErrPool, kind)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint(kind+"/reports"), bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("%w: build %s batch", ErrPool, kind)
	}
	req.Header.Set("Content-Type", "application/json")
	for key, values := range header {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, &PoolError{Status: resp.StatusCode}
	}
	var out struct {
		OK      bool `json:"ok"`
		Results []R  `json:"results"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBatchAckBytes)).Decode(&out); err != nil || !out.OK || len(out.Results) != len(reports) {
		return nil, fmt.Errorf("%w: invalid %s batch acknowledgement", ErrPool, kind)
	}
	for _, result := range out.Results {
		id, status := result.acknowledged()
		if _, exists := ids[id]; !exists || (status != http.StatusOK && (status < 400 || status > 599)) {
			return nil, fmt.Errorf("%w: invalid %s item acknowledgement", ErrPool, kind)
		}
		delete(ids, id)
	}
	return out.Results, nil
}
