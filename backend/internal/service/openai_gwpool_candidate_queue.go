package service

import (
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

// Only names are queued, never a stockpile of live route credentials. Each pick
// reconciles a fresh catalog + local cooldown projection. The FIFO remains the
// exploration/fallback baseline; measured candidates may exchange positions for
// this pick only. A cancelled caller does not reset the baseline order.
type gatewayPoolCandidateQueue struct {
	mu    sync.Mutex
	names []string
}

func (s *openAICodexCookieStore) gatewayPoolCandidateQueue(identity string) *gatewayPoolCandidateQueue {
	value, _ := s.poolCandidateQueues.LoadOrStore(identity, &gatewayPoolCandidateQueue{})
	queue, ok := value.(*gatewayPoolCandidateQueue)
	if !ok || queue == nil {
		panic("invalid gateway pool candidate queue")
	}
	return queue
}

func (q *gatewayPoolCandidateQueue) pick(eligible []gwpool.Gateway, adaptive ...map[string]float64) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	ready := make(map[string]bool, len(eligible))
	for _, candidate := range eligible {
		if candidate.Name != "" {
			ready[candidate.Name] = true
		}
	}
	names := make([]string, 0, len(ready))
	for _, name := range q.names {
		if ready[name] {
			names = append(names, name)
			ready[name] = false
		}
	}
	for _, candidate := range eligible {
		if ready[candidate.Name] {
			names = append(names, candidate.Name)
			ready[candidate.Name] = false
		}
	}
	q.names = names
	if len(names) == 0 {
		return ""
	}
	selected := names[0]
	if len(adaptive) > 0 && len(adaptive[0]) >= 2 {
		ordered := make([]gwpool.Gateway, 0, len(names))
		for _, name := range names {
			ordered = append(ordered, gwpool.Gateway{Name: name})
		}
		selected = rankGatewayPoolAdaptive(ordered, adaptive[0])[0].Name
	}
	// Rotate in the untouched FIFO, not the score order: baseline exploration
	// can still reach a lower-scoring or unmeasured candidate.
	for i, name := range q.names {
		if name == selected {
			q.names = append(append(q.names[:i], q.names[i+1:]...), selected)
			break
		}
	}
	return selected
}

func (q *gatewayPoolCandidateQueue) reset() {
	q.mu.Lock()
	q.names = nil
	q.mu.Unlock()
}
