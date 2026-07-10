package main

import (
	"fmt"
	"time"

	"github.com/peartes/lsm/src/lsm"
)

func main() {
	// scratch main
	db, _ := lsm.Open("/tmp/lsm-check", 42, lsm.Options{FlushThreshold: 1 << 20, MaxQueue: 3}, true)
	for i := range 1000 {
		db.Put(fmt.Sprintf("k-%d", i), []byte("v"))
		db.Get(fmt.Sprintf("k-%d", i))
	}
	// Keep the qq0process alive for scraping.
	time.Sleep(600 * time.Second)
	// http.Handle("/metrics", promhttp.HandlerFor(prom.DefaultGatherer, promhttp.HandlerOpts{}))
	// go http.ListenAndServe(":9090", nil)
}
