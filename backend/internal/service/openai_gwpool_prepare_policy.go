package service

import "context"

type gatewayPoolPreparationIdentityKey struct{}

type gatewayPoolPreparationPolicy struct {
	account *Account
	cancel  context.CancelCauseFunc
}

// Shared ticket work has no row-owned rest threshold. Ordinary waiters apply
// their own policy at candidate boundaries, before the leader starts another
// probe and makes inventory appear busy again.
func (s *openAICodexCookieStore) watchGatewayPoolPreparationPolicy(
	ctx context.Context, identity string, account *Account,
) (context.Context, func()) {
	if account.GatewayPoolContinuousWaitEnabled() {
		return ctx, func() {}
	}
	wait, cancel := context.WithCancelCause(ctx)
	policy := &gatewayPoolPreparationPolicy{account: account, cancel: cancel}
	s.poolPreparePoliciesMu.Lock()
	if s.poolPreparePolicies == nil {
		s.poolPreparePolicies = make(map[string]map[*gatewayPoolPreparationPolicy]struct{})
	}
	if s.poolPreparePolicies[identity] == nil {
		s.poolPreparePolicies[identity] = make(map[*gatewayPoolPreparationPolicy]struct{})
	}
	s.poolPreparePolicies[identity][policy] = struct{}{}
	s.poolPreparePoliciesMu.Unlock()
	return wait, func() {
		s.poolPreparePoliciesMu.Lock()
		delete(s.poolPreparePolicies[identity], policy)
		if len(s.poolPreparePolicies[identity]) == 0 {
			delete(s.poolPreparePolicies, identity)
		}
		s.poolPreparePoliciesMu.Unlock()
		cancel(context.Canceled)
	}
}

func (s *OpenAIGatewayService) applyGatewayPoolPreparationPolicies(ctx context.Context, identity string) {
	store := &s.codexCookies
	store.poolPreparePoliciesMu.Lock()
	policies := make([]*gatewayPoolPreparationPolicy, 0, len(store.poolPreparePolicies[identity]))
	for policy := range store.poolPreparePolicies[identity] {
		policies = append(policies, policy)
	}
	store.poolPreparePoliciesMu.Unlock()
	// Do not hold registry locks during repository/catalog reads, and avoid
	// repeating the same row's reads for many concurrent downstream requests.
	results := make(map[int64]error)
	for _, policy := range policies {
		outcome, checked := results[policy.account.ID]
		if !checked {
			fresh, err := s.freshGatewayPoolPreparationAccount(ctx, policy.account)
			if err != nil {
				continue // unknown supply/health is not exhaustion
			}
			if !gatewayPoolWaitAccountMatches(fresh, policy.account) {
				outcome = errOpenAIGatewayPoolWarmUnverified
			} else if s.gatewayPoolNoRemainingRoutes(ctx, fresh) {
				outcome = errGatewayPoolWarmAttemptsFinished
			}
			results[policy.account.ID] = outcome
		}
		if outcome != nil {
			policy.cancel(outcome)
		}
	}
}
