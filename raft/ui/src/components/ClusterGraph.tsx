import { useEffect, useMemo, useRef, useState, type ReactNode } from "react"
import { ROLE_LEADER, type NodeState, type Peer } from "@/lib/api"
import { cn } from "@/lib/utils"

type Props = {
  peers: Peer[]
  states: Record<number, NodeState>
  partitioned: Record<number, boolean>
  renderNode: (peer: Peer, pct: { left: string; top: string }) => ReactNode
}

type Pos = { id: number; x: number; y: number }

const RADIUS = 22
const CX = 50
// Push the leader-anchor slightly above the viewBox center so the
// downward fan of followers (which contributes most of the y-mass)
// makes the whole shape sit visually centered in its square container.
const CY = 42

// followerAngles returns N angles at which to place followers around the
// leader. Two evenly-spaced points on a circle are diametrically opposite
// (a straight line), which kills the "fan out" look — so the small-N
// cases are hand-tuned for a more pleasing downward fan.
//
// Angles are in SVG convention (y grows down), so π/2 is straight below.
function followerAngles(n: number): number[] {
  if (n === 0) return []
  if (n === 1) return [Math.PI / 2]
  if (n === 2) {
    // ±45° from straight down → V-shape opening upward toward the leader
    return [Math.PI / 2 + Math.PI / 4, Math.PI / 2 - Math.PI / 4]
  }
  // n ≥ 3: regular polygon, starting at the top
  return Array.from({ length: n }, (_, i) => (2 * Math.PI * i) / n - Math.PI / 2)
}

function layoutPositions(
  peers: Peer[],
  states: Record<number, NodeState>,
  partitioned: Record<number, boolean>,
): Pos[] {
  const leader = peers.find(
    (p) => states[p.id]?.role === ROLE_LEADER && !partitioned[p.id],
  )

  if (leader) {
    const followers = peers.filter((p) => p.id !== leader.id)
    const angles = followerAngles(followers.length)
    const out: Pos[] = [{ id: leader.id, x: CX, y: CY }]
    followers.forEach((p, i) => {
      const a = angles[i]
      out.push({
        id: p.id,
        x: CX + RADIUS * Math.cos(a),
        y: CY + RADIUS * Math.sin(a),
      })
    })
    return out
  }

  return peers.map((p, i) => {
    const angle = (2 * Math.PI * i) / peers.length - Math.PI / 2
    return {
      id: p.id,
      x: CX + RADIUS * Math.cos(angle),
      y: CY + RADIUS * Math.sin(angle),
    }
  })
}

// useAnimatedPositions tweens from the current rendered positions to the
// next target positions whenever the target changes. Both nodes and SVG
// edges read from the returned array, which is why they stay glued
// together during a leader change instead of edges snapping while
// circles glide.
//
// Re-running mid-flight is fine: the new tween starts from wherever the
// in-progress tween currently is, so a rapid sequence of leader changes
// produces continuous motion rather than stutter.
function useAnimatedPositions(target: Pos[], duration = 600): Pos[] {
  const [current, setCurrent] = useState<Pos[]>(target)
  const currentRef = useRef<Pos[]>(target)
  currentRef.current = current

  const targetKey = useMemo(
    () =>
      target
        .map((p) => `${p.id}:${p.x.toFixed(2)},${p.y.toFixed(2)}`)
        .join("|"),
    [target],
  )

  useEffect(() => {
    const start = currentRef.current
    const end = target
    const t0 = performance.now()
    let raf = 0

    function tick(now: number) {
      const t = Math.min(1, (now - t0) / duration)
      const e = 0.5 - Math.cos(t * Math.PI) / 2
      const next = end.map((tg) => {
        const s = start.find((p) => p.id === tg.id) ?? tg
        return {
          id: tg.id,
          x: s.x + (tg.x - s.x) * e,
          y: s.y + (tg.y - s.y) * e,
        }
      })
      setCurrent(next)
      if (t < 1) raf = requestAnimationFrame(tick)
    }
    raf = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [targetKey, duration])

  return current
}

function shrinkLine(
  x1: number, y1: number,
  x2: number, y2: number,
  pad: number,
) {
  const dx = x2 - x1
  const dy = y2 - y1
  const len = Math.hypot(dx, dy)
  if (len === 0) return { x1, y1, x2, y2 }
  const ux = dx / len
  const uy = dy / len
  return {
    x1: x1 + ux * pad,
    y1: y1 + uy * pad,
    x2: x2 - ux * pad,
    y2: y2 - uy * pad,
  }
}

function clamp(v: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, v))
}

// DragState tracks one in-progress drag gesture. Lives in a ref so
// pointermove handlers (which run outside React's render cycle) can read
// it cheaply without re-binding.
type DragState = {
  id: number
  startClientX: number
  startClientY: number
  containerRect: DOMRect
  // Cursor offset relative to node center, in viewBox %, so the node
  // doesn't snap-jump to align its center with the cursor on first move.
  offsetPctX: number
  offsetPctY: number
  moved: boolean
}

const DRAG_THRESHOLD_PX = 4

export function ClusterGraph({ peers, states, partitioned, renderNode }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)

  // Manual position overrides set by the user via drag. Persistent across
  // re-renders and even leader changes — what you drag stays where you
  // dropped it. The auto-layout tween still runs underneath but is
  // overridden at render time for any node with a manual position.
  const [manualPositions, setManualPositions] = useState<Record<number, Pos>>(
    {},
  )
  const [draggingID, setDraggingID] = useState<number | null>(null)

  const dragRef = useRef<DragState | null>(null)
  // Set true on a real drag (movement past threshold) and read by the
  // capture-phase click handler to swallow the synthetic click that the
  // browser fires on pointerup. Without this, dragging a node would also
  // open the side panel.
  const justDraggedRef = useRef(false)

  // Tween source: layout-only target. Manual overrides are applied on top
  // at render time so the tween underneath stays consistent and smooth
  // for any node not currently being dragged.
  const layoutTarget = useMemo(
    () => layoutPositions(peers, states, partitioned),
    [peers, states, partitioned],
  )
  const animated = useAnimatedPositions(layoutTarget)
  const positions = animated.map((p) => manualPositions[p.id] ?? p)
  const posByID = new Map(positions.map((p) => [p.id, p]))

  const leader = peers.find(
    (p) => states[p.id]?.role === ROLE_LEADER && !partitioned[p.id],
  )
  const edges =
    leader != null
      ? peers
          .filter(
            (p) =>
              p.id !== leader.id &&
              !partitioned[p.id] &&
              states[p.id]?.reachable,
          )
          .map((p) => ({ from: leader.id, to: p.id }))
      : []

  function onWrapperPointerDown(e: React.PointerEvent, id: number) {
    if (!containerRef.current) return
    // Ignore non-primary buttons (right-click, middle-click).
    if (e.button !== 0) return

    const rect = containerRef.current.getBoundingClientRect()
    const cursorPctX = ((e.clientX - rect.left) / rect.width) * 100
    const cursorPctY = ((e.clientY - rect.top) / rect.height) * 100
    const nodePos = posByID.get(id) ?? { id, x: cursorPctX, y: cursorPctY }

    dragRef.current = {
      id,
      startClientX: e.clientX,
      startClientY: e.clientY,
      containerRect: rect,
      offsetPctX: nodePos.x - cursorPctX,
      offsetPctY: nodePos.y - cursorPctY,
      moved: false,
    }

    function onPointerMove(ev: PointerEvent) {
      const drag = dragRef.current
      if (!drag) return
      const dx = ev.clientX - drag.startClientX
      const dy = ev.clientY - drag.startClientY
      if (!drag.moved && Math.hypot(dx, dy) < DRAG_THRESHOLD_PX) return
      if (!drag.moved) {
        drag.moved = true
        setDraggingID(drag.id)
      }
      const cx =
        ((ev.clientX - drag.containerRect.left) / drag.containerRect.width) *
        100
      const cy =
        ((ev.clientY - drag.containerRect.top) / drag.containerRect.height) *
        100
      // Clamp so users can't fling a node off the canvas.
      const newX = clamp(cx + drag.offsetPctX, 4, 96)
      const newY = clamp(cy + drag.offsetPctY, 4, 96)
      setManualPositions((prev) => ({
        ...prev,
        [drag.id]: { id: drag.id, x: newX, y: newY },
      }))
    }

    function onPointerUp() {
      const drag = dragRef.current
      if (drag?.moved) {
        justDraggedRef.current = true
        // Cleared on the next tick — long enough to swallow the click
        // that fires synchronously after pointerup, short enough not to
        // suppress an unrelated click later.
        setTimeout(() => {
          justDraggedRef.current = false
        }, 0)
      }
      dragRef.current = null
      setDraggingID(null)
      window.removeEventListener("pointermove", onPointerMove)
      window.removeEventListener("pointerup", onPointerUp)
    }

    window.addEventListener("pointermove", onPointerMove)
    window.addEventListener("pointerup", onPointerUp)
  }

  function onWrapperClickCapture(e: React.MouseEvent) {
    if (justDraggedRef.current) {
      e.stopPropagation()
      e.preventDefault()
    }
  }

  return (
    <div
      ref={containerRef}
      className="relative mx-auto aspect-square w-full max-w-[640px] select-none"
    >
      <svg
        viewBox="0 0 100 100"
        preserveAspectRatio="xMidYMid meet"
        className="absolute inset-0 h-full w-full"
      >
        {edges.map(({ from, to }) => {
          const a = posByID.get(from)!
          const b = posByID.get(to)!
          const seg = shrinkLine(a.x, a.y, b.x, b.y, 5)
          return (
            <line
              key={`${from}-${to}`}
              x1={seg.x1}
              y1={seg.y1}
              x2={seg.x2}
              y2={seg.y2}
              className="raft-edge stroke-emerald-400"
              strokeWidth={1.8}
              strokeLinecap="round"
              vectorEffect="non-scaling-stroke"
            />
          )
        })}
      </svg>

      {peers.map((p) => {
        const pos = posByID.get(p.id)!
        return (
          <div
            key={p.id}
            className={cn(
              "absolute touch-none",
              draggingID === p.id ? "cursor-grabbing" : "cursor-grab",
            )}
            style={{
              left: `${pos.x}%`,
              top: `${pos.y}%`,
              transform: "translate(-50%, -50%)",
            }}
            onPointerDown={(e) => onWrapperPointerDown(e, p.id)}
            onClickCapture={onWrapperClickCapture}
          >
            {renderNode(p, { left: `${pos.x}%`, top: `${pos.y}%` })}
          </div>
        )
      })}
    </div>
  )
}
