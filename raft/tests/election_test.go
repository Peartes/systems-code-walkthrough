package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestElectionElectionSimple(t *testing.T) {
	// Create a 3-node cluster.
	// Wait for convergence. Assert exactly one leader exists.
	// Assert all nodes agree on the same term.
	config := make_config(t, 3, false)
	leader := config.checkOneLeader()
	term := config.checkTerms()
	require.NotNil(t, leader, "there should be a leader")
	require.NotEqual(t, term, -1, "there should be a term")
	t.Run("no new leader", func(t *testing.T) {
		time.Sleep(time.Second * 2) // give the network time for any other election to happen (there shouldn't be)
		newLeader := config.checkOneLeader()
		require.Equal(t, newLeader, leader)
		newTerm := config.checkTerms()
		require.Equal(t, newTerm, term)
	})
}

func TestElectionLeaderReElection(t *testing.T) {
	// create a 5 node cluster
	// confirm a leader is elected
	// get the term
	// crash the leader and give the network time to stabilize
	// get the new leader
	// confirm the term of the new leader is strictly greater than the old leaders term
	// confirm the new leader is not the crashed leader
	cfg := make_config(t, 5, false)
	leader := cfg.checkOneLeader()
	require.NotNil(t, leader, "there should be a leader elected already")
	leaderTerm := cfg.checkTerms()
	require.NotEqual(t, leaderTerm, -1, "there should be a common term in the network")
	// crash the leader
	cfg.crash(leader)
	// now get the new leader
	newLeader := cfg.checkOneLeader()
	require.NotNil(t, newLeader, "there should be a new leader in the network")
	require.NotEqual(t, newLeader, leader, "the new leader cannot be the old leader - it has crashed")
	newTerm := cfg.checkTerms()
	require.NotEqual(t, newTerm, -1, "there should be a common term in the network")
	require.Greater(t, newTerm, leaderTerm, "the new term should be strictly greater than the old term")
}

func TestMinorityPartitionNoElection(t *testing.T) {
	// set up a 5 node cluster
	// make sure a leader is elected and the network agrees on a term
	// partition server 3 & 4
	// assert the remaining node still has a leader
	// verify that the partitioned nodes don't have a leader
	cfg := make_config(t, 5, false)
	leader := cfg.checkOneLeader()
	require.NotNil(t, leader, "there should be a leader already")
	term := cfg.checkTerms()
	require.NotEqual(t, term, -1, "there should be an agreed term in the network")
	// partition server 3 & 4
	cfg.disconnect(3)
	cfg.disconnect(4)
	// confirm the majority server still has a leader
	newLeader := cfg.checkOneLeader()
	require.NotNil(t, newLeader, "there should be a leader")
	// make sure partitioned servers can not come to leader election
	raft3 := cfg.rafts[3]
	_, isLeader := raft3.GetState()
	require.False(t, isLeader, "raft3 should not be leader")
	raft4 := cfg.rafts[4]
	_, isLeader = raft4.GetState()
	require.False(t, isLeader, "raft4 should not be leader")
	// connect back the partitioned servers and make sure the network stabilizes
	cfg.connect(3)
	cfg.connect(4)
	newLeader = cfg.checkOneLeader()
	require.NotNil(t, newLeader, "there should be a leader")
}

func TestLeaderDisconnectReconnect(t *testing.T) {
	// create a 5 node cluster
	// confirm a leader is elected and get the term
	// disconnect the leader from the network (it stays alive but can't communicate)
	// wait for the remaining 4 nodes to elect a new leader
	// confirm the new leader is different from the disconnected one
	// confirm the new term is strictly greater than the old term
	// reconnect the old leader
	// confirm the cluster converges to a single leader
	// confirm the old leader has stepped down (it is no longer leader)
	cfg := make_config(t, 5, false)
	leader := cfg.checkOneLeader()
	require.NotNil(t, leader, "there should be a leader elected")
	leaderTerm := cfg.checkTerms()
	require.NotEqual(t, leaderTerm, -1, "there should be an agreed term")
	// disconnect the leader — it stays running but is isolated
	cfg.disconnect(leader)
	// the remaining 4 nodes form a majority and should elect a new leader
	newLeader := cfg.checkOneLeader()
	require.NotNil(t, newLeader, "there should be a new leader after disconnect")
	require.NotEqual(t, newLeader, leader, "the new leader must not be the disconnected node")
	newTerm := cfg.checkTerms()
	require.Greater(t, newTerm, leaderTerm, "the new term should be strictly greater")
	// reconnect the old leader back into the network
	cfg.connect(leader)
	// after reconnect, exactly one leader should exist across all 5 nodes
	finalLeader := cfg.checkOneLeader()
	require.NotNil(t, finalLeader, "there should be a leader after reconnect")
	// the old leader must have stepped down upon receiving a higher-term heartbeat
	oldLeaderTerm, isLeader := cfg.rafts[leader].GetState()
	require.False(t, isLeader, "reconnected old leader must have stepped down")
	require.GreaterOrEqual(t, oldLeaderTerm, newTerm, "old leader must have updated its term")
}
