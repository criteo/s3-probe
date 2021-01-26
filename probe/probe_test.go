package probe

import (
	"log"
	"testing"

	"github.com/criteo/s3-probe/config"
	minio "github.com/minio/minio-go/v6"
)

func TestPrepareBucketCreateBucketIfNotExists(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.latencyBucketName = probe.latencyBucketName + suffix
	probe.durabilityBucketName = probe.durabilityBucketName + suffix
	probe.durabilityItemTotal = 10
	err := probe.prepareLatencyBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}

	exists, _ := probe.endpoint.s3Client.BucketExists(probe.latencyBucketName)
	if !exists {
		t.Errorf("Bucket preparation failed")
	}

	err = probe.prepareLatencyBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}

	err = probe.prepareDurabilityBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
}

func TestPrepareBucketFailedIfNotAuth(t *testing.T) {
	probe, _ := getTestProbe()
	client, _ := minio.New(probe.endpoint.Name, probe.accessKey, "FAKEFAKE", false)
	probe.endpoint.s3Client = client

	suffix, _ := randomHex(8)
	probe.latencyBucketName = probe.latencyBucketName + suffix
	err := probe.prepareLatencyBucket()
	if err == nil {
		t.Errorf("Bucket Creation client's errors are not properly handled")
	}
}

func TestPerformLatencyCheckSuccess(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.latencyBucketName = probe.latencyBucketName + suffix
	probe.durabilityBucketName = probe.durabilityBucketName + suffix
	probe.durabilityItemTotal = 10
	err := probe.prepareDurabilityBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
	err = probe.performDurabilityChecks()
	if err != nil {
		t.Errorf("Probe check is failing: %s", err)
	}
}

func TestDurabilityLatencyCheckSuccess(t *testing.T) {
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

func TestPrepareProbingProperlyTerminate(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	bucket := probe.latencyBucketName
	probe.latencyBucketName = "/./??.."
	err := probe.PrepareProbing()
	if err == nil {
		t.Errorf("Preparation errors are not properly handled: %s", err)
	}
	probe.latencyBucketName = bucket + suffix
	controlChan := probe.controlChan

	controlChan <- false

	err = probe.PrepareProbing()
	if err != nil {
		t.Errorf("Probing is failing: %s", err)
	}
}

func getTestProbe() (Probe, error) {
	endpoint := config.GetEnv("S3_ENDPOINT_ADDR", "localhost:9000")
	service := S3Service{Name: "test", Gateway: false}
	probe, err := NewProbe(service, endpoint, []S3Endpoint{}, config.GetTestConfig(), make(chan bool, 1))
	if err != nil {
		log.Fatalf("Error while creating test env: %s", err)
	}
	_, err = probe.endpoint.s3Client.ListBuckets()
	if err != nil {
		log.Fatalf("Error while creating test env: %s (please set ENV: S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY)", err)
	}
	probe.durabilityItemTotal = 10
	return probe, err
}

func TestPrepareGatewayBucketCreateBucketIfNotExists(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.gatewayBucketName = probe.gatewayBucketName + suffix
	probe.gatewayEndpoints = append(probe.gatewayEndpoints, probe.endpoint)
	err := probe.prepareGatewayBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
	// Preparing an already ready bucket should not result in error
	err = probe.prepareGatewayBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
}

func TestPrepareGatewayBucketCreateBucketFailIfNoGateways(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.gatewayBucketName = probe.gatewayBucketName + suffix
	err := probe.prepareGatewayBucket()
	if err == nil {
		t.Errorf("Bucket didn't fail without gateways")
	}
}

func TestPerformGatewayCheckSuccess(t *testing.T) {
	probe, _ := getTestProbe()
	suffix, _ := randomHex(8)
	probe.gatewayBucketName = probe.gatewayBucketName + suffix
	probe.gatewayEndpoints = append(probe.gatewayEndpoints, probe.endpoint)
	err := probe.prepareGatewayBucket()
	if err != nil {
		t.Errorf("Bucket Creation failed: %s", err)
	}
	err = probe.performGatewayChecks()
	if err != nil {
		t.Errorf("Probe check is failing: %s", err)
	}
}

func TestTimerReturnAFakeTimer(t *testing.T) {
	ticker := newTimer(0)
	if ticker.Ticker != nil {
		t.Errorf("Fake ticker doesn't work")
	}

	ticker.Stop()
}
