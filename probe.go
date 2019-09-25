package main

import (
	"flag"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/s3-probe/watcher"
)

func main() {

	consulAddr := flag.String("consul", "localhost:8500", "Consul server address")
	tag := flag.String("tag", "S3", "Tag to search on consul")
	endpointSuffix := flag.String("suffix", ".service.consul.prod.crto.in:30053",
		"Suffix to add after the consul service name to create a valid domain name")
	latencylatencyBucketName := flag.String("latency-bucket", "monitoring-latency", "Bucket used for the latency monitoring probe (will read and write)")
	durabilityBucketName := flag.String("durability-bucket", "monitoring-durability", "Bucket used for the durability monitoring probe (will read and write)")
	interval := flag.Duration("interval", time.Duration(600_000_000_000), "How often consul is polled to discover new S3 endoints")
	addr := flag.String("listen-address", ":8080", "The address to listen on for HTTP requests.")
	accessKey := flag.String("s3-access-key", "", "User key of the S3 endpoint")
	secretKey := flag.String("s3-secret-key", "", "Access key of the S3 endpoint")
	probeRatePerMin := flag.Int("probe-rate", 120, "Rate of probing per minute (how many checks are done in a minute)")
	durabilityItemSize := flag.Int("item-size", 1024*10, "Size of the item to insert into S3 for durability testing")
	durabilityItemTotal := flag.Int("item-total", 100000, "Total number of items to write into S3 for durabbility testing")
	flag.Parse()
	w := watcher.NewWatcher(*consulAddr, *tag, *endpointSuffix, *latencylatencyBucketName, *durabilityBucketName, *durabilityItemSize, *durabilityItemTotal, *accessKey, *secretKey, *probeRatePerMin)

	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(*addr, nil)
	w.WatchPools(*interval)
}
