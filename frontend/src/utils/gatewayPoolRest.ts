// The existing persisted pool-rest reason is plain text, not a rule error.
export function isGatewayPoolRestReason(reason?: string | null): boolean {
  return !!reason && /^(网关候选低于\d+，|Gateway candidates below \d+;)/.test(reason)
}
