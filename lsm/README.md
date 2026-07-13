# From SkipLists to SSTables: What I Learned Writing an LSM Storage Engine from Scratch

Every call to write() you've ever made was a lie. Until you call fsync, the OS is buffering your data in RAM, and it can vanish on power failure. That's a problem for databases and storage engines. Once a storage engine tells you a write is durable, you have to be able to trust it. That property is called durability, and this post is about what I learned building software that can't afford to lie about it.

## Why I built this and what I built
I have been working in blockchain for over 5 years now and one thing matters most for distributed systems - data durability. Once something is marked committed, it must stay that way. This (like many things) got me thinking of durable databases and how they work so I decided to explore their internals. When people talk about CockroachDB, RocksDB, or LevelDB, I want deep intuition for what they're doing under the hood. More importantly, when I read a Jepsen report next month, I want to actually understand exactly how and why a system is breaking at the disk level.

This project is the child of that exploration. he engine is 6 packages, 190 tests, ~2,500 lines of Go — running on my laptop, crash-safe, concurrent. Every data structure and algorithm written from scratch. To go beyond unit tests, I ran a 10-minute YCSB-B baseline under OTEL + Prometheus instrumentation. More of this in the *Under Load section*. To keep this a learning tool rather than an unfinished lifelong project, I explicitly chose not to build compaction, MVCC (Multi-Version Concurrency Control), concurrent memTable writers, or block compression. By deferring those features, I could focus entirely on the core mechanics of durability and persistence.

## Why LSM exists
Let's look at how storage works. Storage engines have been dominated by B-trees for a long time. Postgres, MySQL, SQLite, most filesystems — B-trees underneath. They're a beautiful data structure: log-N reads, log-N writes, sorted on disk, everything you want.

Everything except one thing: they hate random writes.

A B-tree stores data in fixed-size pages, typically 4–16 KB. When you insert a key, you find the page it belongs on, possibly split it, and write the whole page back in place. One logical insert of 100 bytes triggers one full-page write of, say, 8 KB. If the tree needs rebalancing, it triggers several writes and if inserts arrive in random order — which they usually do — those page writes are scattered all over the disk.

On a spinning disk, that's a disaster. Every random write is a seek. A 5–10 ms mechanical wait for the head to move. Sustained inserts get bottlenecked by the physics of your disk.

On an SSD, it's less obviously bad but arguably worse in aggregate. This is the part that surprised me.

NAND flash has three operations, and they don't compose the way you'd expect.

| Operation	| Granularity	| Notes
| :-------- | :-----------: | -------: |
| Read	| page (4–16 KB) |	Cheap, fast, non-destructive |
| Program (write) |	page |	Can only flip 1→0, never 0→1 |
| Erase	| erase block (256 KB – 4 MB) |	Resets every cell in the block to 1

Do you see the killer problem? Once a cell is programmed, the only way to change it back is to erase the entire block it belongs to. You cannot in-place overwrite. NAND physics won't let you.

So the SSD firmware — the Flash Translation Layer, or FTL — quietly rewrites what "write 4KB at address X" actually means:

- Take the new 4KB. Write it to any fresh, already-erased page somewhere on the device.
- Update a giant in-memory map: "logical address X now lives at new physical page Y."
- Mark the old physical page as stale.

Meanwhile, stale pages accumulate. When enough have piled up in a given erase block, the SSD runs garbage collection: copy the still-valid pages in that erase block elsewhere (more writes!), then erase the whole 256 KB block to reclaim it. Whew — a lot of ceremony for one write.

The consequence is called **write amplification**. Your 100-byte user write can trigger, cumulatively across the SSD's internal machinery, several kilobytes of physical NAND writes. And every write consumes a small amount of NAND cell endurance — flash cells wear out after a bounded number of erase cycles.

The B-tree hits every one of these tender spots. Small random in-place writes are exactly the pattern NAND was designed to hate.

The question that motivates LSM design falls out of this: what if the storage engine at the application layer worked with the grain of the medium instead of against it? What if we only ever appended? What would that database look like? Turns out it's called "an LSM tree"

The rest of this post is what that database looks like — and everything I broke building one.

## The architecture, discovered by necessity
If we only ever appended, what would that look like?

At first, it's simple. Every write goes to the end of a file on disk. No seeks, no in-place updates, no rebalancing — just one syscall to write bytes at the tail of a log. This is the cheapest possible write pattern on every storage medium ever invented. On HDD, the head is already at the tail from the last write. On SSD, sequential appends are what the FTL wants. Everyone's happy.

Well, everyone except reads.

Because if I've written a million records to my log and someone asks "what's the value for key=5?", the honest answer is "I don't know — let me scan a million records to find out." Append-only solves writes by refusing to organize anything.

That's what the rest of this section is about. Every piece of an LSM's architecture is a specific answer to some consequence of "append-only writes make reads terrible."

###### So how do we deal with terrible reads? *"sort in memory"*
The obvious move — sorting the data on disk - doesn't work, because sorting is rearranging, which is the thing we're refusing to do.

But nothing stops us from sorting in RAM. RAM has no seek cost. So the pattern is: keep an in-memory sorted structure, insert into it on every write, and let it grow. When it hits a size threshold, walk it in order and write the whole thing to disk as a sorted file. Then start a new in-memory structure and repeat.

That in-memory structure is called the memtable. In production LSMs, it's usually a skip list — easier to reason about than a red-black tree, and it's naturally friendly to concurrent readers. I used a skip list for the same reasons.

The on-disk file is called an SSTable — Sorted String Table. Once it's written, it never changes. That immutability is going to save us many times.

But hold on, sure reads are now much faster to the order of 0(lg(n)) but what happens if the system crashes before writing out our data to storage ? the memtable will be wiped from RAM and we will realize that we just lost all our photo albums and everyone is very sad.

Users don't tolerate that. When my write returns success, the user assumes the data exists. If a crash later erases it, the user's mental model of the world is now wrong.

The solution is the write-ahead log, or WAL. Before we touch the memtable, we append the operation to a log file on disk. Only after that log entry is durably written do we update the memtable and return success to the user. On restart, we replay the log into a fresh memtable and we're back where we were.

Calling write() on a file does not put the data on disk. The OS buffers the write in RAM (the page cache) and syncs it to disk later, at its convenience. If power fails between your write() and the OS's later flush, the data is gone. This surprised me the first time I really internalized it. Every write() call I'd made in my career had been a small lie — the OS was quietly saying "yeah, I'll get to it."

The syscall that forces the data all the way to disk is fsync. Until fsync returns, you don't get to tell the user the write is durable. Every WAL append in my LSM ends with an fsync. This is the single biggest driver of write latency, and it's the topic of half of the Under Load section below.

At this moment it all made sense. We have a design that does not fight the physics of disk and optimized for writes and read. I wanted to jump into building this myself until I realized while writing the interface that I could not delete a key. If all writes are append-only, how do I delete a key from the database? I cannot re-write the SSTable, that's counter-intuitive and they are immutable anyway. I cannot seek into the WAL to edit it, they are supposed to be append-only. The only thing I am left to do is to make a delete operation a write operation. To delete a key, we simply write a new record that says *"as of now, this key is deleted"* let's call this special write a tombstone.

On reads, when we encounter a tombstone for a key, we return "not found" and stop looking — even if older SSTables have earlier values for that key. The tombstone shadows them.

The strange consequence this brings - deleting a million keys makes our database bigger, not smaller. All we would have done is add a million tombstones. One way to reclaim space is during compaction: if the compactor can prove no older version of a deleted key exists anywhere, it can remove it from the db totally. I didn't build compaction, so my LSM storage gets bigger on every delete.

At this point, we have three places where data lives:

- The active memtable — the fresh in-RAM sorted structure receiving current writes.
- Queued memtables — memtables that hit the size threshold, got frozen, and are waiting for write to disk
- SSTables — sorted immutable files on disk, one per completed flush.

A read has to consult all three. In a specific order: active → queued (newest first) → SSTables (newest first). First match wins. First tombstone wins as "not found."

This is the load-bearing invariant of the entire read path. Get it wrong and old values resurrect from disk to shadow newer deletes. I broke this rule twice while building the system. Both times, the bug was silent — nothing crashed, tests passed, but occasionally a Get returned stale data. I'll get to those in the bugs section.

Reading a single SSTable is fast — it's sorted, so we can binary-search. But after a week of use, there might be hundreds of SSTables on disk. A read for a missing key would need to check every single one to be sure. That's hundreds of disk seeks per "not found." We need a way to reduce the number of SSTable we read. We need a way to filter out SSTable that does not contain a key. For this, instinctively I thought, well, since we are writing out sorted data to SSTable, why not record the min and max key into the table itself, then on read, we can just check if the key falls in the range of the min and max key for that specific SSTable. This ofcourse works but the false positive rate (fpr) as you would imagine is pretty high. Imagine a min/max range of [1, 10⁶]. For any query key inside that range, the range filter says 'maybe' — but if the SSTable actually contains 10,000 keys spread across that range, only 1 in 100 queries in that window is a real hit. The other 99 waste a disk read. We need a better way to reduce the fpr and there is another filter that performs excellently at this - **the bloom filter**

A Bloom filter is a small, in-RAM probabilistic data structure that answers **"is this key possibly in this SSTable?"** It can lie in one direction — *"maybe yes"* when the answer is actually no — but it never says "no" when the answer is "yes." So a "no" is definitive. Bloom filters kill the vast majority of disk reads on missing keys.
The math on Bloom filters is beautiful and I nerded out for an hour on the derivation. Ten bits per key gets you a ~1% false-positive rate with seven hash functions. Every LSM implementer memorizes those two numbers. Learn more about it [here](https://systemdesign.one/bloom-filters-explained/)

So the architecture, arrived at by necessity, looks like this:

**Writes** : WAL append → fsync → memtable insert → return.
**Reads** : active memtable → queued memtables → SSTables newest-first, with Bloom + range filters cutting most SSTable checks.
**Flushes**: memtable full → freeze → background goroutine writes it as a new SSTable → old WAL deleted.

Every piece exists to answer a specific consequence of the append-only choice. None of it is decorative.

The rest of the post is what makes this design brittle — the invariants that must hold, the bugs that broke them, and what it looks like under real load.

## The invariants
Before you jump to read the code or write yours, you must be aware of the invariants of the system. 
1. Reads visit layers newest-first. If you consult an older layer before a newer one, an old value can shadow a fresh one and you serve stale data. I used a monotonically-increasing sequence number on every SSTable and WAL file — cheap and unambiguous
2. WAL-ahead: You only acknowledge a write after you are certain that the data is persisted on disk and this happens after the fsync syscall.
3. You can only delete a WAL segment once you're certain its data is durably in an SSTable. Otherwise the WAL is the only remaining copy and must stay. The manifest is my commit boundary: a flush is committed when the SSTable file is durable AND the manifest lists it. Deleting the WAL is post-commit cleanup, not part of the commit itself.
4. SSTables are immutable. Once Finish() returns, the file never changes. Readers can open it once, keep the fd, safely share across goroutines.

## Subtle Bugs
While building this, I hit some subtle bugs. Here they are, in case they save you time on your own

#### 1. The Bloom filter that ate itself
I named a helper getBloomFilterPos. Sounded pure — a bit array position lookup, no side effects implied. Buried at the bottom of the function was ```blm.table[pos] |= (1 << bit)```. The function was setting the bit as a side effect while pretending to just report where it was.

Add() called it, then re-set the same bit (harmless). MayContain() also called it. Which meant every time I called MayContain(k), I was silently inserting k into the filter. After a few queries for absent keys, the filter would happily start returning "maybe" for everything.

This is one of the worst kinds of bugs: no crash, no error, just wrong answers on some code path. Caught only because a test that queried for an unknown key started returning found=true after other tests ran.

I stared at the failing test for twenty minutes assuming I had a test isolation problem before finally reading the helper function line by line and having the 'oh no' moment

Fix: split the function into a pure posFor() and let the caller do the mutation. Bonus: renamed it so nobody could make the same mistake again.
#### 2. The mid-flush crash + WAL replay race
This one I found by asking "what if?" rather than by seeing it fail. It's the subtlest bug in the codebase.

The scenario: a flush writes the SSTable, updates the manifest to include it, then deletes the old WAL. What if the process crashes between the manifest update and the WAL delete?

On restart, my Open would see the SSTable in the manifest (load it) AND see the WAL still on disk (replay it into a queued memtable). Now the same data lives in two places. The flush goroutine wakes up, sees the queued memtable, and starts flushing it — writing to the same seqno.sst that a Get might already have open. Two writers racing on one file. Undefined behavior.

The fix: split the flush into commit (SSTable file + manifest) and cleanup (WAL delete). Make Open detect the orphan case — WAL seqno already in manifest.LiveSSTables — and delete the WAL without replay. The invariant "an entry is in the SSTables OR the queue, never both" now holds through every crash point.

#### 3. The concurrent rollover race
Design: when the memtable fills up, freeze it, hand it to the flush queue, create a fresh memtable, keep going.

Bug: two Put calls hit the size threshold at the same millisecond, both triggered rollover, both froze the same memtable, both tried to push it to the queue. If either one succeeded, the other pushed a duplicate — a double flush, an orphan WAL, cascading confusion.

The fix wasn't a lock. It was an ordering choice: swap db.active to a fresh memtable before releasing the lock. That way, the second Put arrives, sees the new empty memtable, sees it's below threshold, and just does a normal write. The first rollover finishes its bookkeeping while the second Put has already moved on.

This is the kind of concurrency bug you don't find by running tests. I only spotted it by walking through what the second goroutine would see at each line.

## Under Load
Correctness tests prove the code doesn't lie. They don't tell you what it feels like to use.

For that, I wanted numbers. Real p50s, p99s, p999s. Not vibes, not guesses, but a distribution measured over a sustained run. So I wired up OpenTelemetry — a latency histogram for each of Get, Put, Delete, plus operation counters — exported to Prometheus via the OTEL Prometheus exporter. Grafana on top, scraping every 5 seconds, rendering percentiles and heatmaps.

For workload, I wrote a small YCSB-B driver — about 200 lines of Go (AI scaffolded it quickly). YCSB-B is Yahoo's read-heavy benchmark, 95% reads and 5% updates against a Zipfian-skewed key space. Zipfian is the point: it mimics real workloads (a hot set of keys accessed constantly, a cold tail rarely touched), which stresses the LSM's cache paths the way a uniform distribution wouldn't.

Then I ran it: 10,000 records loaded, 4 worker goroutines, 10 minutes.

#### The headline numbers
Read throughput (steady state): **~2,500 ops/s**
Write throughput: **~130 ops/s**
Read p50: below the **50-microsecond** bucket (I can't see through the floor of my own histogram)
Read p99: **~8 ms** (measured 8.1–8.5 ms across two runs)
Read p999: **~10 ms**
Write p50: **~3 ms**
Write p99: **~8 ms** (measured 8.1 ms)
Write p999: **~10 ms**, with one spike to ~22 ms mid-run

Read and write latency heatmap
![read_write_latency_heatmap](https://raw.githubusercontent.com/Peartes/first-principles/refs/heads/main/lsm/deploy/data/Screenshot%202026-07-10%20at%2004.52.34.png)

#### Reads are bimodal

The read heatmap is the strongest visual. Two distinct bright bands with a dark valley between them.

The bottom band, at 0–50 microseconds, is the memtable hit population — every read that found its key in the in-memory skip list. Almost instant. The Zipfian hot set lives here almost permanently, so most reads land here.

The upper band, at 1–25 milliseconds, is the SSTable read population — reads that missed the memtable and had to bloom-check, seek into an SSTable, and parse a block off disk. This band is much fainter because only a small fraction of reads make it this far. But it's there, and it's ~100× slower than the memtable band.

The dark valley between them — 100 microseconds to 1 millisecond — is not a rendering artifact. Nothing lives in that range. You either hit RAM (fast) or hit disk (slow); there's no in-between path. When I read Jepsen reports that mention "bimodal latency distributions" as a smell, this is the picture in my head now.

This was the first time I saw a bimodal distribution in something I'd built and it was really awesome to see. The percentile graph hid this detail also. If the workload ran longer, the bimodal shape would become even more pronounced — hot keys forming the first band, tail keys the second

#### Writes are unimodal, and fsync sets the floor
Refer back to the latency heatmap

The writes have a different shape entirely. One narrow band, centered at 2.5–5 milliseconds, holding almost every write.

That's the fsync floor. Every WAL append ends with fsync, and fsync on my laptop's NVMe takes about 3 milliseconds. There is no faster write than that while preserving durability. You can add group commit to amortize the fsync across many writes, or put the WAL on a faster device, or drop the durability guarantee — but you can't make a single fsync go faster than the disk lets it.

The tightness of the band is the visual proof: every write pays the same fsync tax. No fast path, no slow path, just the physics of the storage device.

Read and write latency percentile
![read_write_latency_percentile](https://raw.githubusercontent.com/Peartes/first-principles/refs/heads/main/lsm/deploy/data/Screenshot%202026-07-10%20at%2004.52.20.png)

#### Read p99 and Write p99 both converged at ~8 ms
Look at the numbers: read p99 is ~8 ms, write p99 is ~8 ms.

That's not a coincidence since this test is by the same physical device. The read tail is dominated by SSTable disk reads (bloom miss → seek + read a block off disk). The write tail is dominated by fsync jitter — the moments when fsync has to wait behind a bigger fsync from the flush goroutine. Different code paths, different syscalls, but the same underlying hardware — and they converge on the same latency at the p99.

The disk is the physics of this system. Both tails eventually collide with it.

#### The flush contention spike
Around 04:17 in the run, the write p999 spiked to about 22 ms for about 40 seconds. Read latency was flat. Throughput held steady.

What happened: my background flush goroutine finished writing an SSTable and called sstable.Finish(), which fsyncs an entire memtable's worth of data — a big burst compared to a single WAL append's fsync. The disk serviced that big fsync first. My four worker goroutines' small WAL fsyncs queued behind it at the block layer. Each Put took a few milliseconds longer during that window; none was fully blocked.

Reads were untouched because reads don't fsync. They use pread-style syscalls that don't wait for write barriers. And 95% of reads don't touch disk at all — the memtable was still serving them.

This is exactly why production LSMs put the WAL on a separate device (RocksDB style), or batch multiple Puts into a single fsync (Postgres group commit). I did neither. The spike in my graph is a real, direct visualization of the problem those systems are solving.

#### Throughput grew 8× over the run
![throughput](https://raw.githubusercontent.com/Peartes/first-principles/refs/heads/main/lsm/deploy/data/Screenshot%202026-07-10%20at%2004.52.53.png)

Reads/sec started at about 300 and climbed to about 2,500 over the 10 minutes. The workload literally sped up as it ran.

Two effects compounded. The OS page cache warmed — as reads hit the same SSTables over and over, the disk blocks ended up in RAM, and subsequent reads that "went to disk" actually got served from cache. And the Zipfian hot key set stabilized in the memtable, because updates keep rewriting the same hot keys, and those overwrites live in memory until flush time.

The workload got faster as the caches learned it. Which is exactly what caches are supposed to do. But it's also a reminder that "steady state" is a longer-tail concept than a 10-minute benchmark can fully capture. If I'd left this running for 10 hours, throughput would have kept climbing until SSTable count started to hurt the miss path.

Which is a story about compaction. Which I didn't build. Which is a story for the next section.

## Real Limitations
While this has been fun and insightful, there are missing components that make this still a toy
- Get holds the database lock during SSTable disk I/O — writers stall while a read is in flight. A fix (and maybe a follow-up post): snapshot state under the lock, iterate outside
- Seqno is per-file, not per-record. Production systems tag every record with a global monotonic seqno for compaction + MVCC.
- No compaction. Real LSMs are more compaction than any other component. Without it, my SSTables accumulate and the read p99 would drift up over hours — which the 10-minute benchmark can't reveal but a 10-hour one would.
- Manifest is a single atomically-renamed file. Real systems use append-only manifest logs with periodic snapshots — more robust against partial writes.
- No block compression, no per-block CRC on SSTables — just per-record CRC on the WAL. Fine for a toy; not for production.
- No group commit. Every Put pays one fsync. Postgres and MySQL batch multiple transactions per fsync and amortize the ~3 ms cost. My write p50 would drop 5–10× with group commit.

## Conclusion
If you made it this far, it's great to have you. I appreciate you taking the time. If you skipped all the way down, you missed the juice up top. This has been an interesting journey for me and if you do decide to build one, it will be for you too. This isn't just some random build, this storage engine will sit on my previous Raft implementation to make it into a distributed k/v store. My next step is to implement distributed transactions on this and unlock sharding. Now, go build yours.
