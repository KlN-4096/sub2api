// One confirmation keeps its UUID across transport failures and component/page
// reloads. Replacing it after an unknown outcome could spend a second card.
const uuidV4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/
const storageKey = (identity: string) => `cpr-reset-request:${encodeURIComponent(identity)}`

export function readPendingCPRResetRequest(identity: string): string | null {
  const existing = sessionStorage.getItem(storageKey(identity))
  return existing && uuidV4.test(existing) ? existing : null
}

export function pendingCPRResetRequest(identity: string): string {
  const existing = readPendingCPRResetRequest(identity)
  if (existing) return existing
  const id = crypto.randomUUID()
  // A storage failure must happen before sending a non-refundable operation.
  sessionStorage.setItem(storageKey(identity), id)
  return id
}

export function completeCPRResetRequest(identity: string): void {
  sessionStorage.removeItem(storageKey(identity))
}
