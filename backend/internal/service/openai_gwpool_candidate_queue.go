package service

import (
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

// Only names are queued, never a stockpile of live route credentials. Each pick
// reconciles a fresh catalog + local cooldown projection. The FIFO remains the
// exploration/fallback baseline; ranking splits it into dynamic quality/ordinary
// queues for this pick only. A cancelled caller does not reset the baseline order.
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

func (q *gatewayPoolCandidateQueue) pick(eligible []gwpool.Gateway, policy ...gatewayPoolCandidateRanking) string {
	q.mu.Lock()
	defer q.mu.Unlock()
	ordered := gatewayPoolReconcileCandidates(q.names, eligible)
	q.names = make([]string, len(ordered))
	for i, candidate := range ordered {
		q.names[i] = candidate.Name
	}
	if len(ordered) == 0 {
		return ""
	}
	selected := ordered[0].Name
	if len(policy) > 0 {
		selected = policy[0].order(ordered)[0].Name
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

// Reconcile a private view without rotating, admitting work, or storing names.
func (q *gatewayPoolCandidateQueue) preview(eligible []gwpool.Gateway) []gwpool.Gateway {
	q.mu.Lock()
	defer q.mu.Unlock()
	return gatewayPoolReconcileCandidates(q.names, eligible)
}

func gatewayPoolReconcileCandidates(queued []string, eligible []gwpool.Gateway) []gwpool.Gateway {
	ready := make(map[string]bool, len(eligible))
	catalog := make(map[string]gwpool.Gateway, len(eligible))
	for _, candidate := range eligible {
		if candidate.Name != "" {
			ready[candidate.Name] = true
			catalog[candidate.Name] = candidate
		}
	}
	names := make([]string, 0, len(ready))
	for _, name := range queued {
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
	ordered := make([]gwpool.Gateway, 0, len(names))
	for _, name := range names {
		ordered = append(ordered, catalog[name])
	}
	return ordered
}

func (q *gatewayPoolCandidateQueue) reset() {
	q.mu.Lock()
	q.names = nil
	q.mu.Unlock()
}
