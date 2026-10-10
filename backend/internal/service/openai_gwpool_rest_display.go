package service

import "time"

type GatewayPoolRestView struct {
	Active    bool      `json:"active"`
	ChangedAt time.Time `json:"changed_at,omitzero"`
	StartedAt time.Time `json:"started_at,omitzero"`
	ResumeAt  time.Time `json:"resume_at,omitzero"`
	NextCheck time.Time `json:"next_check,omitzero"`
	Reason    string    `json:"reason,omitempty"`
}

// Read-only projection of the admission latch. The local timer publishes the
// inactive tombstone durably; the browser must not infer a successful write.
func (s *OpenAIGatewayService) gatewayPoolRestDisplay(account *Account, identity string, peers []Account) GatewayPoolRestView {
	if account.GatewayPoolContinuousWaitEnabled() {
		return GatewayPoolRestView{}
	}
	state := s.codexCookies.gatewayPoolRestSnapshot(account, identity, peers)
	view := GatewayPoolRestView{Active: state.Active, ChangedAt: state.ChangedAt, StartedAt: state.StartedAt,
		ResumeAt: state.ResumeAt, NextCheck: state.NextCheck}
	if view.Active {
		view.Reason = gatewayPoolRestReason(account, state.QualityGateways...)
	}
	return view
}

func (s *openAICodexCookieStore) gatewayPoolRestSnapshot(account *Account, identity string, peers []Account) gatewayPoolRestState {
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
	if cached, ok := s.poolRestState.Load(tag); ok {
		if other, valid := cached.(gatewayPoolRestState); valid {
			adopt(other)
		}
	}
	return state
}
