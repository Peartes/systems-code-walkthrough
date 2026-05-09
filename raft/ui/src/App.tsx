import { useEffect, useState } from "react"
import { TooltipProvider } from "@/components/ui/tooltip"
import { NodeCircle } from "@/components/NodeCircle"
import { SidePanel } from "@/components/SidePanel"
import { ClusterGraph } from "@/components/ClusterGraph"
import { SeedPage } from "@/components/SeedPage"
import {
  fetchPeers,
  fetchState,
  type NodeState,
  type Peer,
} from "@/lib/api"

const POLL_MS = 1000

export default function App() {
  const [peers, setPeers] = useState<Peer[] | null>(null)
  const [states, setStates] = useState<Record<number, NodeState>>({})
  // Locally-tracked partition state per node. The backend's GetState does
  // not echo this back (the partition flag lives only in cmd/node), so the
  // dashboard treats its own click-history as the source of truth for the
  // visual ring overlay. Refreshing the page resets it — acceptable for
  // a manual demo tool.
  const [partitioned, setPartitioned] = useState<Record<number, boolean>>({})
  const [selectedID, setSelectedID] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Load the cluster shape once.
  useEffect(() => {
    fetchPeers()
      .then(setPeers)
      .catch((e) => setError(`failed to load peers: ${e.message}`))
  }, [])

  // Poll every node's state at POLL_MS.
  useEffect(() => {
    if (!peers) return
    let cancelled = false
    async function tick() {
      const ps = peers!
      const results = await Promise.all(
        ps.map((p) =>
          fetchState(p.id).catch(
            (): NodeState => ({
              id: p.id,
              term: 0,
              role: 0,
              commitIndex: 0,
              lastApplied: 0,
              logs: [],
              reachable: false,
            }),
          ),
        ),
      )
      if (cancelled) return
      const next: Record<number, NodeState> = {}
      for (const s of results) next[s.id] = s
      setStates(next)
    }
    tick()
    const handle = setInterval(tick, POLL_MS)
    return () => {
      cancelled = true
      clearInterval(handle)
    }
  }, [peers])

  function onPartitionChange(id: number, p: boolean) {
    setPartitioned((prev) => ({ ...prev, [id]: p }))
  }

  if (error) {
    return (
      <div className="flex h-full items-center justify-center text-rose-400">
        {error}
      </div>
    )
  }
  if (!peers) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground">
        loading peers…
      </div>
    )
  }
  if (peers.length === 0) {
    // Empty config.json — first run, or after a manual reset. Show the
    // seed prompt so the user can write the cluster shape from the UI.
    return (
      <TooltipProvider delayDuration={200}>
        <SeedPage onSeeded={setPeers} />
      </TooltipProvider>
    )
  }

  const selectedState = selectedID != null ? states[selectedID] : undefined

  return (
    <TooltipProvider delayDuration={200}>
      <div className="flex h-full flex-col">
        <header className="border-b border-border px-6 py-4">
          <h1 className="text-xl font-semibold tracking-tight">
            Raft Cluster
          </h1>
          <p className="text-xs text-muted-foreground">
            polling every {POLL_MS}ms · click a node for details
          </p>
        </header>

        <div className="flex flex-1 gap-8 overflow-hidden px-8 py-12">
          <main className="flex flex-1 items-center justify-center">
            <ClusterGraph
              peers={peers}
              states={states}
              partitioned={partitioned}
              renderNode={(p) => {
                const s = states[p.id]
                if (!s) return null
                return (
                  <NodeCircle
                    state={s}
                    partitioned={!!partitioned[p.id]}
                    selected={selectedID === p.id}
                    onClick={() => setSelectedID(p.id)}
                  />
                )
              }}
            />
          </main>

          {selectedState && (
            <aside className="overflow-y-auto">
              <SidePanel
                state={selectedState}
                partitioned={!!partitioned[selectedState.id]}
                onPartitionChange={onPartitionChange}
              />
            </aside>
          )}
        </div>
      </div>
    </TooltipProvider>
  )
}
