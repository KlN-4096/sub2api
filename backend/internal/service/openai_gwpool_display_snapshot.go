package service

import "time"

// Read-only merge for the admin display. Preserve the catalog's original
// UpdatedAt; polling a local snapshot is not a fresh external pool observation.
func (s *OpenAIGatewayService) gatewayPoolDisplaySnapshot(
	account *Account, identity string, peers []Account,
) (openAIGatewayHistory, gatewayPoolContacts) {
	tag := gatewayPoolLedgerTag(identity)
	history := openAIGatewayHistory{LedgerTag: tag, Seen: map[string]openAIGatewaySeen{}}
	contacts := gatewayPoolContacts{LedgerTag: tag, Seen: map[string]gatewayPoolContactSeen{}}
	merge := func(row *Account) {
		record, _ := readOpenAIGatewayHistory(row)
		if matching := gatewayPoolCooldownResetHistory(&record, tag); matching != nil {
			if matching.UpdatedAt.After(history.UpdatedAt) {
				history.Current, history.CurrentRegion = matching.Current, matching.CurrentRegion
				history.PoolLive, history.PoolFree, history.UpdatedAt = matching.PoolLive, matching.PoolFree, matching.UpdatedAt
			}
			if matching.CooldownReset.LastAt.After(history.CooldownReset.LastAt) {
				history.CooldownReset = matching.CooldownReset
			}
			for gateway, seen := range matching.Seen {
				old, exists := history.Seen[gateway]
				cooldown := old.Cooldown
				if newerGatewayPoolCooldown(seen.Cooldown, cooldown) {
					cooldown = seen.Cooldown
				}
				if !exists || seen.At.After(old.At) {
					history.Seen[gateway] = seen
				}
				current := history.Seen[gateway]
				current.Cooldown = cooldown
				history.Seen[gateway] = current
			}
		}
		mergeGatewayPoolContacts(&contacts, readGatewayPoolContacts(row, tag))
	}
	merge(account)
	for i := range peers {
		if peers[i].ID != account.ID {
			merge(&peers[i])
		}
	}
	for gateway, seen := range history.Seen {
		if current, ok := s.codexCookies.cooldownEntry(identity, gateway); ok &&
			newerGatewayPoolCooldown(&current, seen.Cooldown) {
			seen.Cooldown = &current
			history.Seen[gateway] = seen
		}
	}
	pruneGatewayPoolContacts(&contacts, time.Now())
	return history, contacts
}
