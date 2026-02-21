## Example 1: Word Selection and Counting

**Purpose:** Demonstrates race condition between task that produces data and task that consumes it.

**Non-deterministic behavior:**
- Sometimes counter runs before selector → counts wrong word
- Sometimes selector runs first → counter gets correct word
- Order varies each run

**Deterministic behavior:**
- Same seed always produces same ordering
- Can test both "good" and "bad" orderings reliably
- Seed 42: selector first (correct)
- Seed 157: counter first (incorrect)

**Blockchain relevance:** 
Smart contract reads state that another contract is modifying. Transaction ordering matters!

## Example 2: CPU-Bound Task Starvation

**Purpose:** Shows how one uncooperative task can block all others.

**Setup:**
- Task A: Greedy (no yields, long computation)
- Task B: Cooperative (yields regularly)
- Task C: I/O-bound (mostly waiting)

**Results:**

| Runtime | Task A Time | Task B Time | Task C Time |
|---------|-------------|-------------|-------------|
| Tokio Multi | 2.0s | 2.0s | 0.1s | ← Parallel
| Tokio Single | 2.0s | 4.0s | 4.1s | ← Sequential
| Commonware | 2.0s | 4.0s | 4.1s | ← Sequential

**Key insight:** Multi-threading hides the starvation problem. Single-threaded exposes it.

**Fix:** Add yields to Task A every N iterations.

## Example 3: Race Condition in Concurrent Increment

**Purpose:** Classic race condition that deterministic testing can reliably reproduce.

**Bug:** 10 tasks increment shared counter, but final value is 3-9 (not 10).

**Root cause:** Read-modify-write without synchronization.

**Discovery:**
- Tokio: Bug appears randomly (~30% of runs)
- Commonware: Bug appears with specific seeds (42, 157, 273, 388...)

**Once seed is known:** Can reproduce 100% of the time for debugging.

**Fix options:**
1. Use proper synchronization (Mutex)
2. Use atomic operations
3. Serialize increments (no concurrency)