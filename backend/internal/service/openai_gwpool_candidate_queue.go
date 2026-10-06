package service

import (
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/gwpool"
)

// Only names are queued, never a stockpile of live route credentials. Each pick
// reconciles a fresh catalog + local cooldown projection. Returning candidates
// join the tail; a cancelled caller does not reset the other candidates' order.
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

func (q *gatewayPoolCandidateQueue) pick(eligible []gwpool.Gateway) string {
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
	q.names = append(q.names[1:], selected)
	return selected
}

func (q *gatewayPoolCandidateQueue) reset() {
	q.mu.Lock()
	q.names = nil
	q.mu.Unlock()
}
