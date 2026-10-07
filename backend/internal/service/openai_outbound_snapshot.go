package service

// Snapshot only mutable outbound configuration; unrelated scheduling state is
// never changed here. Delayed WS prewarm and quota calls must not retain maps
// that a token refresh or an administrator update can replace in place.
func snapshotOpenAIOutboundAccount(account *Account) *Account {
	snapshot := snapshotOAuthRefreshAccount(account)
	if snapshot == nil {
		return nil
	}
	snapshot.Extra = shallowCopyMap(account.Extra)
	if account.Proxy != nil {
		proxy := *account.Proxy
		snapshot.Proxy = &proxy
	}
	if account.ParentAccountID != nil {
		parentID := *account.ParentAccountID
		snapshot.ParentAccountID = &parentID
	}
	return snapshot
}
