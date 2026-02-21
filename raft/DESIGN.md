# Summary

This is an implementation of the Raft consensus algorithm in golang. Raft is one of the most understanable consensus protocol I have read so far and frankly it's interesting to implement. This project takes a step further by actually using the implementation as the consensus layer to a KV store.

# Scope
There are multiple layers to the raft algorithm to make it fully production ready. This implementation focuses on the core concept required for consensus. The implemented components are:
- Leader Election
- Log replication
- Safety
Some out of scope components are:
- Follower and candidate crashes
- Timing and availability
- Cluster membership changes
- Log compaction

# Invariants
For anyone unknowing, invariants are things that must always be true throughout the whole program execution. For raft, the invariants are in the safety properties of Raft:
- Only one leader per term
- If the content of a log at a term and index is correct, then every log in all preceeding term and index must be correct
- Leader completeness: committed entries always present in future leaders
- State machine safety: servers apply same entry at same index

## Other notes
- Servers have most of their storage in memory with only data required for correctness being stored in storage