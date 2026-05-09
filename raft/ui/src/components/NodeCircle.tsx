import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import { Badge } from "@/components/ui/badge"
import {
  ROLE_LEADER,
  ROLE_CANDIDATE,
  type NodeState,
} from "@/lib/api"
import { cn } from "@/lib/utils"

type Props = {
  state: NodeState
  partitioned: boolean
  selected: boolean
  onClick: () => void
}

function roleColor(state: NodeState, partitioned: boolean) {
  if (!state.reachable) return "bg-zinc-700"
  if (partitioned) return "bg-zinc-600"
  if (state.role === ROLE_LEADER) return "bg-emerald-500"
  if (state.role === ROLE_CANDIDATE) return "bg-amber-400"
  return "bg-zinc-400"
}

function roleLabel(state: NodeState) {
  if (!state.reachable) return "Unreachable"
  if (state.role === ROLE_LEADER) return "Leader"
  if (state.role === ROLE_CANDIDATE) return "Candidate"
  return "Follower"
}

export function NodeCircle({ state, partitioned, selected, onClick }: Props) {
  const isLivePLeader =
    state.reachable && !partitioned && state.role === ROLE_LEADER

  // last log index = entries excluding the engine's sentinel at idx 0.
  // For an empty/unreachable node we render "—" so a glance at the
  // badge always says "what's the head of your log".
  const lastLogIdx =
    state.reachable && state.logs.length > 0 ? state.logs.length - 1 : null

  // Face label rules:
  //   reachable → the term (so a stale node visually pops out against
  //               its peers — exactly what you want at a glance).
  //   unreachable → em-dash, since term has no meaning while we can't
  //                 talk to the node.
  const faceLabel = state.reachable ? `${state.term}` : "—"

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div className="relative flex flex-col items-center">
          <button
            onClick={onClick}
            aria-label={`Node ${state.id}: ${roleLabel(state)} (term ${state.term})`}
            className={cn(
              // Base size 56px (graphdb-like). Hover bumps modestly to
              // ~62px (scale 1.1) — present but not distracting.
              "group relative flex size-14 shrink-0 cursor-pointer items-center justify-center rounded-full text-sm font-semibold text-zinc-950 shadow-md transition-all duration-200 ease-out",
              roleColor(state, partitioned),
              !state.reachable && "opacity-40",
              partitioned &&
                "ring-2 ring-rose-500 ring-offset-2 ring-offset-background",
              selected &&
                "outline-2 outline-offset-2 outline-foreground/70",
              "hover:-translate-y-0.5 hover:scale-110 hover:shadow-xl active:scale-100",
              isLivePLeader && "raft-leader-pulse",
            )}
          >
            <span className="font-mono text-lg leading-none tabular-nums">
              {faceLabel}
            </span>
          </button>

          {/* Log-index badge below the circle. Absolutely positioned so the
              hover scale doesn't displace it. "i<last-index>" reads as
              "this node has committed up through array index N". */}
          {lastLogIdx != null && (
            <Badge
              variant="secondary"
              className="pointer-events-none absolute top-[calc(100%+6px)] h-4 rounded-full bg-card/80 px-1.5 py-0 font-mono text-[10px] tracking-tight text-muted-foreground"
            >
              i{lastLogIdx}
            </Badge>
          )}
        </div>
      </TooltipTrigger>
      <TooltipContent>
        <div className="text-xs">
          <div>
            node {state.id} — {roleLabel(state)}
          </div>
          {state.reachable && (
            <div className="text-muted-foreground">
              term {state.term} · commit {state.commitIndex} · {state.logs.length} logs
            </div>
          )}
          {partitioned && <div className="text-rose-400">partitioned</div>}
        </div>
      </TooltipContent>
    </Tooltip>
  )
}
