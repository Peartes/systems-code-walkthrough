package main

import "github.com/peartes/lsm/src/lsm"

// LSMAdapter wraps the LSM to expose the three operations YCSB-B needs.
// The workload/load code calls these three methods and doesn't know
// anything about the underlying storage.
//
// You (yes, you reading this) implement the three functions below.
// They should each be a single call into db.<Put|Get|Delete|whatever>.
//
// Contract:
//
//   - Load(key, value) is called during the LOAD phase, once per record
//     inserted from the workload's synthetic key space. First time we've
//     seen this key. Return an error if the insert failed.
//
//   - Read(key) is called during the RUN phase whenever the workload
//     selects a read op. Return (value, nil) on success. If the key
//     doesn't exist, return (nil, nil) — a miss is not an error; the run
//     phase counts them alongside hits so the p99 latency reflects the
//     actual mix (misses are usually SLOWER than hits because they force
//     the read path to iterate every SSTable). Return (nil, err) only
//     for genuine storage errors.
//
//   - Update(key, value) is called during the RUN phase whenever the
//     workload selects an update. The key was inserted during load; the
//     value has changed. Return an error on failure.
type LSMAdapter struct {
	db *lsm.DB
}

func (a *LSMAdapter) Load(key string, value []byte) error {
	return a.db.Put(key, value)
}

func (a *LSMAdapter) Read(key string) ([]byte, error) {
	val, found, err := a.db.Get(key)
	if !found {
		return nil, nil
	}
	return val, err
}

func (a *LSMAdapter) Update(key string, value []byte) error {
	return a.db.Put(key, value)
}
