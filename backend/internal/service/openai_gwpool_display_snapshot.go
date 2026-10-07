package service

import "time"

type gatewayPoolDisplayReadKey struct {
	account *Account
	tag     string
}

type gatewayPoolDisplayRecord struct {
	history  openAIGatewayHistory
	contacts gatewayPoolContacts
}

// Lifetime is one admin snapshot request, keyed by the actual row snapshot,
// not ID: a fresh row and a peer query may contain different values for one ID.
type gatewayPoolDisplayCache map[gatewayPoolDisplayReadKey]gatewayPoolDisplayRecord

func (cache gatewayPoolDisplayCache) read(account *Account, tag string) gatewayPoolDisplayRecord {
	key := gatewayPoolDisplayReadKey{account: account, tag: tag}
	if record, ok := cache[key]; ok {
		return record
	}
	history, _ := readOpenAIGatewayHistory(account)
	record := gatewayPoolDisplayRecord{history: history, contacts: readGatewayPoolContacts(account, tag)}
	if cache != nil {
		cache[key] = record
	}
	return record
}

// Read-only merge for the admin display. Preserve the catalog's original
// UpdatedAt; polling a local snapshot is not a fresh external pool observation.
func (s *OpenAIGatewayService) gatewayPoolDisplaySnapshot(
	account *Account, identity string, peers []Account, caches ...gatewayPoolDisplayCache,
) (openAIGatewayHistory, gatewayPoolContacts) {
	var cache gatewayPoolDisplayCache
	if len(caches) > 0 {
		cache = caches[0]
	}
	tag := gatewayPoolLedgerTag(identity)
	history := openAIGatewayHistory{LedgerTag: tag, Seen: map[string]openAIGatewaySeen{}}
	contacts := gatewayPoolContacts{LedgerTag: tag, Seen: map[string]gatewayPoolContactSeen{}}
	merge := func(row *Account) {
		record := cache.read(row, tag)
		if matching := gatewayPoolCooldownResetHistory(&record.history, tag); matching != nil {
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
		// addAlias may append to a slice in a copied round. Give this projection
		// its own alias backing arrays so another row cannot mutate cached data.
		other := record.contacts
		other.Rounds = append([]gatewayPoolContactRound(nil), other.Rounds...)
		for i := range other.Rounds {
			other.Rounds[i].Aliases = append([]string(nil), other.Rounds[i].Aliases...)
		}
		mergeGatewayPoolContacts(&contacts, other)
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
