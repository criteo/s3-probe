package probe

import (
	"log"
	"os"
	"testing"

	minio "github.com/minio/minio-go/v6"
)

func TestPrepareBucketCreateBucketIfNotExists(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.latencyBucketName = probe.latencyBucketName + suffix
	err := probe.prepareLatencyBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}

	exists, _ := probe.s3Client.BucketExists(probe.latencyBucketName)
	if !exists {
		t.Errorf("Bucket preparation failed")
	}

	err = probe.prepareLatencyBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
}

func TestPrepareBucketFailedIfNotAuth(t *testing.T) {
	probe, _ := getTestProbe()
	client, _ := minio.New(probe.endpoint, probe.accessKey, "FAKEFAKE", false)
	probe.s3Client = client

	suffix, _ := randomHex(8)
	probe.latencyBucketName = probe.latencyBucketName + suffix
	err := probe.prepareLatencyBucket()
	if err == nil {
		t.Errorf("Bucket Creation client's errors are not properly handled")
	}
}

func TestperformCheckSuccess(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.latencyBucketName = probe.latencyBucketName + suffix
	err := probe.prepareLatencyBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
	err = probe.performLatencyChecks()
	if err != nil {
		t.Errorf("Probe check is failing: %s", err)
	}
}

func TestStartProbingProperlyTerminate(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	bucket := probe.latencyBucketName
	probe.latencyBucketName = "/./??.."
	err := probe.StartProbing()
	if err == nil {
		t.Errorf("Preparation errors are not properly handled: %s", err)
	}
	probe.latencyBucketName = bucket + suffix
	controlChan := probe.controlChan

	controlChan <- false

	err = probe.StartProbing()
	if err != nil {
		t.Errorf("Probing is failing: %s", err)
	}
}

func getTestProbe() (Probe, error) {
	endpoint := getEnv("S3_ENDPOINT", "localhost:9000")
	accessKey := getEnv("S3_ACCESS_KEY", "9PWM3PGAOU5TESTINGKEY")
	secretKey := getEnv("S3_SECRET_KEY", "p4KQAm5cLKfW2QoJG8SI5JOI3gYSECRETKEY")
	probe, err := NewProbe(endpoint, "", accessKey, secretKey, "monitoring-latency-test", "monitoring-durab-test", 300, 10, 10, make(chan bool, 1))
	if err != nil {
		log.Fatalf("Error while creating test env: %s", err)
	}
	_, err = probe.s3Client.ListBuckets()
	if err != nil {
		log.Fatalf("Error while creating test env: %s (please set ENV: S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY)", err)
	}
	probe.durabilityItemTotal = 10
	return probe, err
}

func getEnv(env string, defaultVal string) string {
	val := os.Getenv(env)
	if val == "" {
		val = defaultVal
	}
	return val
}
