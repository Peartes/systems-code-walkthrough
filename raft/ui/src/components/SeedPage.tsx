import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Separator } from "@/components/ui/separator"
import { MAX_SEED_COUNT, seedCluster, type Peer } from "@/lib/api"

type Props = {
  onSeeded: (peers: Peer[]) => void
}

// SeedPage is shown when /api/peers returns []. It writes a fresh
// config.json with N peer entries (sequential ports starting at
// 18500/18600/18700) but does NOT start any processes — the user is
// expected to run `make nodes-up` from a terminal afterwards.
export function SeedPage({ onSeeded }: Props) {
  const [count, setCount] = useState(3)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  async function handleSeed() {
    setBusy(true)
    setError(null)
    try {
      const peers = await seedCluster(count)
      onSeeded(peers)
    } catch (e) {
      setError((e as Error).message)
    } finally {
      setBusy(false)
    }
  }

  const valid = Number.isInteger(count) && count >= 1 && count <= MAX_SEED_COUNT

  return (
    <div className="flex h-full items-center justify-center p-6">
      <Card className="w-[28rem]">
        <CardHeader>
          <CardTitle>Seed your cluster</CardTitle>
        </CardHeader>
        <CardContent className="space-y-5">
          <p className="text-sm text-muted-foreground">
            Pick how many nodes the cluster should have. This writes{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              config.json
            </code>{" "}
            with that many peer entries. No processes are started — run{" "}
            <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
              make nodes-up
            </code>{" "}
            from your terminal afterwards.
          </p>

          <Separator />

          <div className="space-y-2">
            <label className="text-xs font-semibold uppercase text-muted-foreground">
              Node count (1–{MAX_SEED_COUNT})
            </label>
            <div className="flex items-center gap-3">
              <Input
                type="number"
                min={1}
                max={MAX_SEED_COUNT}
                value={count}
                onChange={(e) => setCount(parseInt(e.target.value, 10) || 0)}
                className="w-24 font-mono text-base"
                disabled={busy}
              />
              <div className="flex gap-1">
                {[3, 5, 7, 10].map((n) => (
                  <Button
                    key={n}
                    variant={count === n ? "default" : "outline"}
                    size="sm"
                    onClick={() => setCount(n)}
                    disabled={busy}
                  >
                    {n}
                  </Button>
                ))}
              </div>
            </div>
          </div>

          <div className="max-h-32 overflow-y-auto rounded-md border border-border bg-card/40 p-3 text-xs font-mono text-muted-foreground">
            {Array.from({ length: Math.max(0, Math.min(count, MAX_SEED_COUNT)) }).map(
              (_, i) => (
                <div key={i}>
                  node {i} · grpc :{18500 + i} · metrics :{18700 + i}
                </div>
              ),
            )}
            {!valid && (
              <div className="text-rose-400">
                pick an integer between 1 and {MAX_SEED_COUNT}
              </div>
            )}
          </div>

          {error && (
            <div className="rounded-md border border-rose-700/40 bg-rose-950/30 p-2 text-xs text-rose-300">
              {error}
            </div>
          )}

          <Button
            onClick={handleSeed}
            disabled={!valid || busy}
            className="w-full"
          >
            {busy ? "writing config.json…" : `Seed ${count} nodes`}
          </Button>
        </CardContent>
      </Card>
    </div>
  )
}
