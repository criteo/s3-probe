package main

import (
	"net/http"

	"github.com/criteo/s3-probe/pkg/config"
	"github.com/criteo/s3-probe/pkg/watcher"

	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(200)
}

func main() {
	cfg := config.ParseConfig()
	w := watcher.NewWatcher(cfg)

	http.HandleFunc("/ready", healthCheck)
	http.Handle("/metrics", promhttp.Handler())

	go http.ListenAndServe(*cfg.Addr, nil)
	w.WatchPools(*cfg.Interval)
}
