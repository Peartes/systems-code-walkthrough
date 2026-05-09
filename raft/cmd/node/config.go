package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// PeerConfig describes a single Raft node in the cluster. Loaded from
// config.json; the same file is shared by every node so each one can build
// the full peer slice without runtime discovery.
type PeerConfig struct {
	ID          int    `json:"id"`
	GRPCAddr    string `json:"grpc_addr"`
	APIAddr     string `json:"api_addr"`
	MetricsAddr string `json:"metrics_addr"`
}

// Config is the JSON structure of config.json.
type Config struct {
	Peers []PeerConfig `json:"peers"`
}

// loadConfig reads and validates the cluster config. Validation rules:
//   - non-empty peer list
//   - peer IDs form the contiguous range [0, len(Peers)) — required because
//     raft.Make uses the slice index as the node identity, so a sparse or
//     out-of-order ID list would silently break replication
func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(c.Peers) == 0 {
		return nil, fmt.Errorf("config has no peers")
	}
	seen := make(map[int]bool, len(c.Peers))
	for _, p := range c.Peers {
		if p.ID < 0 || p.ID >= len(c.Peers) {
			return nil, fmt.Errorf("peer id %d out of range [0,%d)", p.ID, len(c.Peers))
		}
		if seen[p.ID] {
			return nil, fmt.Errorf("duplicate peer id %d", p.ID)
		}
		if p.GRPCAddr == "" {
			return nil, fmt.Errorf("peer %d has empty grpc_addr", p.ID)
		}
		seen[p.ID] = true
	}
	return &c, nil
}

// peerByID returns the entry for id, or nil if not present.
func (c *Config) peerByID(id int) *PeerConfig {
	for i := range c.Peers {
		if c.Peers[i].ID == id {
			return &c.Peers[i]
		}
	}
	return nil
}
