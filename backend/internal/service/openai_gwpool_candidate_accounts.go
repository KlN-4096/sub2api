package service

import "context"

func gatewayPoolDedupeCandidates(ctx context.Context, accounts []*Account, protected ...int64) []*Account {
	identities, _ := ctx.Value(gatewayPoolCandidateDomainsKey{}).(map[int64]string)
	if len(identities) == 0 {
		return accounts
	}
	owners := map[string]int64{}
	for _, account := range accounts {
		if identity := identities[account.ID]; identity != "" {
			if owner := owners[identity]; owner == 0 || account.ID < owner {
				owners[identity] = account.ID
			}
		}
	}
	// Existing weighted sticky/previous-response and retry-only rows retain
	// their exact binding, even when another clone is the usual representative.
	protected = append([]int64{gatewayPoolRetryOnlyFrom(ctx).accountID}, protected...)
	claimed := map[string]bool{}
	for _, id := range protected {
		for _, account := range accounts {
			identity := identities[id]
			if account.ID == id && identity != "" && !claimed[identity] {
				owners[identity], claimed[identity] = id, true
			}
		}
	}
	out := make([]*Account, 0, len(accounts))
	for _, account := range accounts {
		identity := identities[account.ID]
		if identity == "" || owners[identity] == account.ID {
			out = append(out, account)
		}
	}
	return out
}

func withGatewayPoolStickyPreference(ctx context.Context, ids ...int64) context.Context {
	protected := map[int64]bool{}
	for _, id := range ids {
		if id > 0 {
			protected[id] = true
		}
	}
	return context.WithValue(ctx, gatewayPoolStickyPreferenceKey{}, protected)
}
