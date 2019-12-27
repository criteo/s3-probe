package config

import (
	"flag"
	"os"
	"time"
)

// Config contains the configuration of the probe
type Config struct {
	ConsulAddr           *string
	Tag                  *string
	GatewayTag           *string
	EndpointSuffix       *string
	LatencyBucketName    *string
	GatewayBucketName    *string
	DurabilityBucketName *string
	Interval             *time.Duration
	Addr                 *string
	AccessKey            *string
	SecretKey            *string
	ProbeRatePerMin      *int
	DurabilityItemSize   *int
	DurabilityItemTotal  *int
}

// ParseConfig parse the configuration and create a Config struct
func ParseConfig() Config {
	config := Config{
		ConsulAddr: flag.String("consul", "localhost:8500", "Consul server address"),
		Tag:        flag.String("tag", "s3", "Tag to search on consul"),
		GatewayTag: flag.String("GatewayTag", "s3-gateway", "Tag to search on consul"),
		EndpointSuffix: flag.String("suffix", ".service.{dc}.consul.preprod.crto.in",
			"Suffix to add after the consul service name to create a valid domain name"),
		LatencyBucketName:    flag.String("latency-bucket", "monitoring-latency", "Bucket used for the latency monitoring probe (will read and write)"),
		GatewayBucketName:    flag.String("gateway-bucket", "monitoring-gateway", "Bucket used for the gateway latency monitoring probe (will read and write)"),
		DurabilityBucketName: flag.String("durability-bucket", "monitoring-durability", "Bucket used for the durability monitoring probe (will read and write)"),
		Interval:             flag.Duration("interval", time.Duration(600_000_000_000), "How often consul is polled to discover new S3 endoints"),
		Addr:                 flag.String("listen-address", ":8080", "The address to listen on for HTTP requests."),
		AccessKey:            flag.String("s3-access-key", "", "User key of the S3 endpoint"),
		SecretKey:            flag.String("s3-secret-key", "", "Access key of the S3 endpoint"),
		ProbeRatePerMin:      flag.Int("probe-rate", 120, "Rate of probing per minute (how many checks are done in a minute)"),
		DurabilityItemSize:   flag.Int("item-size", 1024*10, "Size of the item to insert into S3 for durability testing"),
		DurabilityItemTotal:  flag.Int("item-total", 100000, "Total number of items to write into S3 for durabbility testing"),
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
	probeRatePerMin := 300
	durabilityItemSize := 10
	durabilityItemTotal := 10
	interval := time.Duration(1)

	return Config{
		ConsulAddr:           &dummyValue,
		Tag:                  &dummyValue,
		GatewayTag:           &dummyValue,
		EndpointSuffix:       &dummyValue,
		LatencyBucketName:    &latencyBucketName,
		GatewayBucketName:    &latencyBucketName,
		DurabilityBucketName: &durabilityBucketName,
		Interval:             &interval,
		Addr:                 &dummyValue,
		ProbeRatePerMin:      &probeRatePerMin,
		DurabilityItemSize:   &durabilityItemSize,
		DurabilityItemTotal:  &durabilityItemTotal,

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
