package config

import (
	"flag"
	"os"
	"time"
)

// Config contains the configuration of the probe
type Config struct {
	ConsulAddr                *string
	Tag                       *string
	GatewayTag                *string
	EndpointSuffix            *string
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
}

// ParseConfig parse the configuration and create a Config struct
func ParseConfig() Config {
	config := Config{
		ConsulAddr: flag.String("consul", "localhost:8500", "Consul server address"),
		Tag:        flag.String("tag", "s3", "Tag to search on consul"),
		GatewayTag: flag.String("gateway-tag", "s3-gateway", "Tag to search on consul"),
		EndpointSuffix: flag.String("suffix", ".service.{dc}.foo.bar",
			"Suffix to add after the consul service name to create a valid domain name"),
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
	}

	flag.Parse()
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

	return Config{
		ConsulAddr:                &dummyValue,
		Tag:                       &dummyValue,
		GatewayTag:                &dummyValue,
		EndpointSuffix:            &dummyValue,
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
