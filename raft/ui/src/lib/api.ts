// Thin client for the Go dashboard backend (cmd/dashboard).
// Vite proxies /api -> http://localhost:8080 in dev, so URLs are origin-relative.

export type Peer = {
  id: number
  grpc_addr: string
  api_addr: string
  metrics_addr: string
}

export type LogEntry = {
  term: number
  entry: string
}

// role values match the iota in internal/raft/state.go: Follower=0, Candidate=1, Leader=2.
export const ROLE_FOLLOWER = 0
export const ROLE_CANDIDATE = 1
export const ROLE_LEADER = 2

export type NodeState = {
  id: number
  term: number
  role: 0 | 1 | 2
  commitIndex: number
  lastApplied: number
  logs: LogEntry[]
  reachable: boolean
}

export async function fetchPeers(): Promise<Peer[]> {
  const r = await fetch("/api/peers")
  if (!r.ok) throw new Error(`peers: ${r.status}`)
  // Backend returns null for an empty/missing config; normalise to [].
  return (await r.json()) ?? []
}

// MAX_SEED_COUNT mirrors cmd/dashboard's MaxSeedCount. Prometheus uses
// file-based service discovery so there's no extra cap on the scrape
// side — bump both this and the backend constant in lockstep if you
// ever need a bigger cluster.
export const MAX_SEED_COUNT = 25

export async function seedCluster(count: number): Promise<Peer[]> {
  const r = await fetch("/api/seed", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ count }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => ({ error: r.statusText }))
    throw new Error(body.error ?? `seed: ${r.status}`)
  }
  return r.json()
}

export async function fetchState(id: number): Promise<NodeState> {
  const r = await fetch(`/api/nodes/${id}/state`)
  if (!r.ok) throw new Error(`state ${id}: ${r.status}`)
  const j = await r.json()
  // Empty logs come back as null over JSON; normalise to [] so the UI
  // never has to null-check.
  return { ...j, logs: j.logs ?? [] }
}

export async function submitCommand(id: number, command: string) {
  const r = await fetch(`/api/nodes/${id}/command`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ command }),
  })
  if (!r.ok) throw new Error(`command: ${r.status}`)
  return r.json() as Promise<{ index: number; term: number; isLeader: boolean }>
}

export async function setPartitioned(id: number, partitioned: boolean) {
  const r = await fetch(`/api/nodes/${id}/partition`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ partitioned }),
  })
  if (!r.ok) throw new Error(`partition: ${r.status}`)
  return r.json() as Promise<{ partitioned: boolean }>
}
