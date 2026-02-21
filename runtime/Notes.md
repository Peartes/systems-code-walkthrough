# Deterministic Task Scheduler Demonstrator

Understanding async runtimes, cooperative scheduling, and deterministic execution for blockchain systems.

## What This Project Explores

Most blockchain engineers use async runtimes without understanding how they work. This project explores the layer below VMs and execution environments - the task scheduler that determines **when** and **how** code runs.

### Key Questions Answered

1. **Why do blockchains need deterministic execution?**
   - Validators must agree on execution order, not just final state
   - Non-deterministic scheduling breaks consensus

2. **How does cooperative scheduling fail?**
   - Greedy tasks can starve others
   - Race conditions depend on execution order
   - One malicious contract can DoS the entire chain

3. **How do we test distributed systems reliably?**
   - Deterministic simulation with seed-based execution
   - Reproduce bugs 100% of the time
   - Test 1000+ orderings in seconds

## Key Findings

### 1. Multi-threading Hides Problems

Tokio's default multi-threaded runtime masks starvation issues because greedy tasks only block one thread. Single-threaded runtimes expose these problems immediately.

### 2. Determinism ≠ Same Output

Deterministic execution means same **execution path** (order of operations), not just same final result. This is critical for consensus.

### 3. Seeds Enable Debugging

Once you find a seed that triggers a bug, you can reproduce it 100% of the time. This transforms "random CI failures" into "reproducible test cases."

### 4. Blockchain Execution is Single-Threaded

Even though validators use multi-threaded runtimes in production, the execution model is fundamentally sequential for determinism.

## Lessons for Blockchain Development

1. **Gas metering is forced cooperation** - Without it, malicious contracts would starve honest ones
2. **Transaction ordering is consensus-critical** - Not just an optimization
3. **Deterministic simulation finds bugs** - Test thousands of orderings before production
4. **Same state ≠ valid execution** - Validators must agree on path, not just destination

## Comparison: Tokio vs Commonware

| Aspect | Tokio | Commonware |
|--------|-------|------------|
| Determinism | ❌ | ✅ |
| Performance | High (multi-threaded) | Medium (single-threaded) |
| Use Case | Production | Testing/Simulation |
| Bug Reproduction | Hard | Easy |

## Resources

- [Commonware Documentation](https://commonware.xyz)
- [Tokio Documentation](https://tokio.rs)

## Author
Kehinde Faleye
Built as part of a learning sprint on blockchain runtime systems.

**Key Technologies:** Rust, Tokio, Commonware Runtime, Async/Await

**Focus Areas:** Distributed systems, consensus, deterministic execution
```