package config

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

// Config contains the configuration of the probe
type Config struct {
	ConsulAddr                *string
	StaticS3Endpoints         map[string]string
	GatewayTag                *string
	LatencyBucketName         *string
	GatewayBucketName         *string
	DurabilityBucketName      *string
	Interval                  *time.Duration
	Addr                      *string
	AccessKey                 *string
	SecretKey                 *string
	ProbeRatePerMin           *int
	DurabilityProbeRatePerMin *int
	LatencyItemSize           *int
	DurabilityItemSize        *int
	DurabilityItemTotal       *int
	DurabilityTimeout         *time.Duration
	LatencyTimeout            *time.Duration
	CleanupDelay              *time.Duration
}

// ParseConfig parse the configuration and create a Config struct
func ParseConfig() Config {

	staticS3EndpointsJson := flag.String("static-s3-endpoints", "{}", "JSON map name -> endpoint of declarative S3 endpoints to monitor")

	config := Config{
		ConsulAddr:                flag.String("consul", "", "Consul server address. When empty, falls back to the CONSUL_HTTP_ADDR env var, then to 127.0.0.1:8500"),
		GatewayTag:                flag.String("gateway-tag", "s3-gateway", "Tag to search on consul"),
		LatencyBucketName:         flag.String("latency-bucket", "monitoring-latency", "Bucket used for the latency monitoring probe (will read and write)"),
		GatewayBucketName:         flag.String("gateway-bucket", "monitoring-gateway", "Bucket used for the gateway latency monitoring probe (will read and write)"),
		DurabilityBucketName:      flag.String("durability-bucket", "monitoring-durability", "Bucket used for the durability monitoring probe (will read and write)"),
		Interval:                  flag.Duration("interval", 600*time.Second, "How often consul is polled to discover new S3 endoints"),
		DurabilityTimeout:         flag.Duration("durablity-timeout", 60*time.Second, "Timeout duration of the durability check"),
		LatencyTimeout:            flag.Duration("latency-timeout", 30*time.Second, "Timeout duration of the latency check"),
		Addr:                      flag.String("listen-address", ":8080", "The address to listen on for HTTP requests."),
		AccessKey:                 flag.String("s3-access-key", "", "User key of the S3 endpoint"),
		SecretKey:                 flag.String("s3-secret-key", "", "Access key of the S3 endpoint"),
		ProbeRatePerMin:           flag.Int("probe-rate", 120, "Rate of probing per minute (how many checks are done in a minute)"),
		DurabilityProbeRatePerMin: flag.Int("durability-probe-rate", 1, "Rate of probing per minute (how many checks are done in a minute)"),
		DurabilityItemSize:        flag.Int("durability-item-size", 1024*10, "Size of the item to insert into S3 for durability testing"),
		LatencyItemSize:           flag.Int("latency-item-size", 1024*10, "Size of the item to insert into S3 for latency testing"),
		DurabilityItemTotal:       flag.Int("item-total", 100000, "Total number of items to write into S3 for durability testing"),
		CleanupDelay:              flag.Duration("cleanup-delay", 30*time.Second, "Delay before deleting objects created during probing"),
	}

	flag.Parse()

	err := json.Unmarshal([]byte(*staticS3EndpointsJson), &config.StaticS3Endpoints)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing -static-s3-endpoints: %v\n", err)
		os.Exit(1)
	}

	return config
}

func GetTestConfig() Config {
	dummyValue := ""
	accessKey := GetEnv("S3_ACCESS_KEY", "9PWM3PGAOU5TESTINGKEY")
	secretKey := GetEnv("S3_SECRET_KEY", "p4KQAm5cLKfW2QoJG8SI5JOI3gYSECRETKEY")
	latencyBucketName := "monitoring-latency-test"
	durabilityBucketName := "monitoring-durab-test"
	probeRatePerMin := 120
	durabilityProbeRatePerMin := 1
	latencyItemSize := 10
	durabilityItemSize := 10
	durabilityItemTotal := 10
	interval := time.Duration(1)
	durabilityTimeout := time.Duration(60_000_000_000)
	latencyTimeout := time.Duration(5_000_000_000)
	cleanupDelay := time.Duration(0)

	return Config{
		ConsulAddr:                &dummyValue,
		GatewayTag:                &dummyValue,
		LatencyBucketName:         &latencyBucketName,
		GatewayBucketName:         &latencyBucketName,
		DurabilityBucketName:      &durabilityBucketName,
		Interval:                  &interval,
		Addr:                      &dummyValue,
		ProbeRatePerMin:           &probeRatePerMin,
		DurabilityProbeRatePerMin: &durabilityProbeRatePerMin,
		LatencyItemSize:           &latencyItemSize,
		DurabilityItemSize:        &durabilityItemSize,
		DurabilityItemTotal:       &durabilityItemTotal,
		DurabilityTimeout:         &durabilityTimeout,
		LatencyTimeout:            &latencyTimeout,
		CleanupDelay:              &cleanupDelay,

		AccessKey: &accessKey,
		SecretKey: &secretKey,
	}
}

func GetEnv(env string, defaultVal string) string {
	val := os.Getenv(env)
	if val == "" {
		val = defaultVal
	}
	return val
}
