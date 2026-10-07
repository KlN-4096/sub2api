package service

import "time"

type GatewayPoolRestView struct {
	Active    bool      `json:"active"`
	ChangedAt time.Time `json:"changed_at,omitzero"`
	NextCheck time.Time `json:"next_check,omitzero"`
	Reason    string    `json:"reason,omitempty"`
}

// Read-only projection of the same identity-scoped latch as admission. A recheck
// deadline is not an expiry; only a newer inactive tombstone ends this status.
func (s *OpenAIGatewayService) gatewayPoolRestDisplay(account *Account, identity string, peers []Account) GatewayPoolRestView {
	tag := gatewayPoolLedgerTag(identity)
	state := readGatewayPoolRest(account, tag)
	adopt := func(other gatewayPoolRestState) {
		if other.ChangedAt.After(state.ChangedAt) {
			state = other
		}
	}
	for i := range peers {
		adopt(readGatewayPoolRest(&peers[i], tag))
	}
	if cached, ok := s.codexCookies.poolRestState.Load(tag); ok {
		if other, valid := cached.(gatewayPoolRestState); valid {
			adopt(other)
		}
	}
	view := GatewayPoolRestView{Active: state.Active, ChangedAt: state.ChangedAt, NextCheck: state.NextCheck}
	if view.Active {
		view.Reason = gatewayPoolRestReason(account)
	}
	return view
}
