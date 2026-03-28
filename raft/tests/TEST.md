# Overview 
This is a complete test harness to control the environment of the raft processes. 
To simulate failure scenarios, we must control time, network and number of nodes, mode of delivery e.t.c.


The Five Capabilities It Must Have
1. Spin up N nodes
Create a 3 or 5 node cluster entirely in memory. Each node is a real Raft state machine, running real goroutines, but communicating through our controlled network instead of real TCP.
2. Partition nodes
Cut communication between specific nodes. Node 0 cannot send to or receive from nodes 1 and 2. This simulates a network partition — the most important failure scenario in distributed systems. This is how to test split-brain prevention.
3. Heal partitions
Restore communication. Now test that the cluster recovers correctly — the partitioned node's log gets overwritten by the new leader's log, it doesn't keep its own divergent state.
4. Kill and restart nodes
Stop a node's goroutines entirely, then restart it from its persisted state. This tests that the persistence layer is correct — the node must recover to the right term and log state after restart.
5. Control message delivery
Drop a percentage of messages, delay them, or reorder them. This surfaces bugs that only appear under unreliable network conditions — which is most bugs in consensus implementations.