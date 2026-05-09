import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Separator } from "@/components/ui/separator"
import {
  ROLE_LEADER,
  ROLE_CANDIDATE,
  submitCommand,
  setPartitioned,
  type LogEntry,
  type NodeState,
} from "@/lib/api"
import { cn } from "@/lib/utils"

type Props = {
  state: NodeState
  partitioned: boolean
  onPartitionChange: (id: number, partitioned: boolean) => void
}

function roleBadge(state: NodeState) {
  if (!state.reachable) {
    return <Badge variant="secondary">Unreachable</Badge>
  }
  if (state.role === ROLE_LEADER) {
    return <Badge className="bg-emerald-500 text-zinc-950">Leader</Badge>
  }
  if (state.role === ROLE_CANDIDATE) {
    return <Badge className="bg-amber-400 text-zinc-950">Candidate</Badge>
  }
  return <Badge variant="outline">Follower</Badge>
}

export function SidePanel({ state, partitioned, onPartitionChange }: Props) {
  const [command, setCommand] = useState("")
  const [submitMsg, setSubmitMsg] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function onSubmit() {
    if (!command.trim()) return
    setBusy(true)
    setSubmitMsg(null)
    try {
      const r = await submitCommand(state.id, command)
      if (r.isLeader) {
        setSubmitMsg(`accepted at index ${r.index} (term ${r.term})`)
        setCommand("")
      } else {
        setSubmitMsg(`rejected — node ${state.id} is not the leader`)
      }
    } catch (e) {
      setSubmitMsg(`error: ${(e as Error).message}`)
    } finally {
      setBusy(false)
    }
  }

  async function onTogglePartition() {
    setBusy(true)
    try {
      const next = !partitioned
      await setPartitioned(state.id, next)
      onPartitionChange(state.id, next)
    } catch (e) {
      setSubmitMsg(`error: ${(e as Error).message}`)
    } finally {
      setBusy(false)
    }
  }

  return (
    <Card className="w-96">
      <CardHeader>
        <CardTitle className="flex items-center justify-between">
          <span>node {state.id}</span>
          {roleBadge(state)}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-3 gap-2 text-sm">
          <Stat label="term" value={state.term} />
          <Stat label="commit" value={state.commitIndex} />
          <Stat label="applied" value={state.lastApplied} />
        </div>

        <Separator />

        <div>
          <h3 className="mb-2 text-sm font-semibold uppercase text-muted-foreground">
            Log entries ({state.logs.length})
          </h3>
          <LogBoxes logs={state.logs} commitIndex={state.commitIndex} />
        </div>

        <Separator />

        <div className="space-y-2">
          <h3 className="text-sm font-semibold uppercase text-muted-foreground">
            Submit command
          </h3>
          <div className="flex gap-2">
            <Input
              value={command}
              onChange={(e) => setCommand(e.target.value)}
              placeholder="e.g. set foo=bar"
              onKeyDown={(e) => {
                if (e.key === "Enter") onSubmit()
              }}
              disabled={busy || !state.reachable}
            />
            <Button onClick={onSubmit} disabled={busy || !state.reachable || !command.trim()}>
              Send
            </Button>
          </div>
          {submitMsg && (
            <div className="text-xs text-muted-foreground">{submitMsg}</div>
          )}
        </div>

        <Separator />

        <div className="space-y-2">
          <h3 className="text-sm font-semibold uppercase text-muted-foreground">
            Partition (incoming Raft RPCs)
          </h3>
          <Button
            variant={partitioned ? "destructive" : "outline"}
            onClick={onTogglePartition}
            disabled={busy || !state.reachable}
            className="w-full"
          >
            {partitioned ? "Heal partition" : "Partition this node"}
          </Button>
          <p className="text-xs text-muted-foreground">
            Symmetric: both inbound and outbound Raft RPCs are dropped.
          </p>
        </div>
      </CardContent>
    </Card>
  )
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md bg-card/40 p-2 text-center">
      <div className="text-xs uppercase text-muted-foreground">{label}</div>
      <div className="font-mono text-lg tabular-nums">{value}</div>
    </div>
  )
}

// LogBoxes renders the Raft log as a horizontally-scrollable strip of
// small boxes. The committed/pending distinction is carried entirely by
// the border — strong solid for committed entries (visually "locked"),
// dashed for pending ones the leader has appended but hasn't yet
// replicated to a majority.
//
// The sentinel entry at index 0 (term -1, empty payload) renders as an
// em-dash inside its box; the rest show the entry's term number.
function LogBoxes({
  logs,
  commitIndex,
}: {
  logs: LogEntry[]
  commitIndex: number
}) {
  if (logs.length === 0) {
    return (
      <div className="rounded-md border border-border bg-card/40 p-3 text-xs text-muted-foreground">
        — empty —
      </div>
    )
  }
  return (
    <div className="rounded-md border border-border bg-card/40 p-3">
      <div className="raft-thin-scroll flex gap-1.5 overflow-x-auto pb-2">
        {logs.map((entry, idx) => {
          const committed = idx <= commitIndex
          const sentinel = entry.term < 0
          return (
            <div
              key={idx}
              className="flex flex-col items-center"
              title={entry.entry ? entry.entry : sentinel ? "sentinel" : ""}
            >
              <div
                className={cn(
                  // 32px squares — small enough to fit ~14 across before
                  // scrolling, large enough to read a 2-digit term.
                  "flex h-8 w-8 shrink-0 items-center justify-center rounded-md font-mono text-xs tabular-nums transition-colors",
                  committed
                    ? // Solid strong border + bold weight — visually
                      // "settled" without needing color.
                      "border-2 border-foreground/70 font-bold text-foreground"
                    : // Dashed thin border + normal weight — clearly
                      // provisional, hasn't earned its keep yet.
                      "border border-dashed border-muted-foreground/60 font-normal text-muted-foreground",
                  sentinel && "text-muted-foreground",
                )}
              >
                {sentinel ? "—" : entry.term}
              </div>
              <div className="mt-1 font-mono text-[10px] tabular-nums text-muted-foreground">
                {idx}
              </div>
            </div>
          )
        })}
      </div>
      <div className="mt-1 flex items-center gap-3 text-[10px] text-muted-foreground">
        <span className="flex items-center gap-1.5">
          <span className="inline-block size-2.5 rounded-sm border-2 border-foreground/70" />
          committed (≤ {commitIndex})
        </span>
        <span className="flex items-center gap-1.5">
          <span className="inline-block size-2.5 rounded-sm border border-dashed border-muted-foreground/60" />
          pending
        </span>
      </div>
    </div>
  )
}
