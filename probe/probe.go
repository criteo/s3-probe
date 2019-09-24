package probe

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var s3LatencySummary = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "s3_latency_seconds",
	Help: "Latency for operation on the S3 endpoint",
}, []string{"operation", "endpoint"})

var s3TotalCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_request_total",
	Help: "Total number of requests on S3 endpoint",
}, []string{"operation", "endpoint"})

var s3SuccessCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_request_success_total",
	Help: "Total number of successful requests on S3 endpoint",
}, []string{"operation", "endpoint"})

const millisecondInMinute = 60_000

// Probe is a S3 probe
type Probe struct {
	name            string
	endpoint        string
	secretKey       string
	accessKey       string
	bucketName      string
	probeRatePerMin int
	s3Client        *minio.Client
	controlChan     chan bool
}

// NewProbe creates a new S3 probe
func NewProbe(name string, suffix string, accessKey string, secretKey string, bucketName string, probeRatePerMin int, controlChan chan bool) (Probe, error) {
	endpoint := name + suffix
	minioClient, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		return Probe{}, err
	}

	log.Println("Probe created for:", endpoint)
	return Probe{
		name:            name,
		endpoint:        endpoint,
		secretKey:       secretKey,
		accessKey:       accessKey,
		bucketName:      bucketName,
		probeRatePerMin: probeRatePerMin,
		controlChan:     controlChan,
		s3Client:        minioClient,
	}, nil
}

// StartProbing start to probe the S3 endpoint
func (p *Probe) StartProbing() error {

	err := p.prepareBucket()
	if err != nil {
		log.Println("Error: cannot prepare bucket:", err)
		return err
	}

	log.Println("Starting probing")
	for {
		select {
		// If we receive something on the control chan we terminate
		// otherwise we continue to perform checks
		case <-p.controlChan:
			log.Println("Terminating probe on", p.name)
			return nil
		case <-time.After(time.Duration(millisecondInMinute/p.probeRatePerMin) * time.Millisecond):
			go p.performCheck()
		}
	}
}

func (p *Probe) performCheck() error {
	objectName, _ := randomHex(20)
	objectSize := int64(1024)

	operation := func() error {
		_, err := p.s3Client.ListBuckets()
		return err
	}
	if err := p.mesureOperation("list_buckets", operation); err != nil {
		return err
	}

	objectData, _ := randomObject(objectSize)
	operation = func() error {
		_, err := p.s3Client.PutObject(p.bucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		return err
	}
	if err := p.mesureOperation("put_object", operation); err != nil {
		return err
	}

	operation = func() error {
		_, err := p.s3Client.GetObject(p.bucketName, objectName, minio.GetObjectOptions{})
		return err
	}
	if err := p.mesureOperation("get_object", operation); err != nil {
		return err
	}

	operation = func() error {
		err := p.s3Client.RemoveObject(p.bucketName, objectName)
		return err
	}
	if err := p.mesureOperation("remove_object", operation); err != nil {
		return err
	}

	return nil
}

func (p *Probe) mesureOperation(operationName string, operation func() error) error {
	start := time.Now()
	err := operation()

	s3TotalCounter.WithLabelValues(operationName, p.name).Inc()
	s3LatencySummary.WithLabelValues(operationName, p.name).Observe(time.Since(start).Seconds())

	if err != nil {
		log.Printf("Error while executing %s: %s", operationName, err)
		return err
	}
	s3SuccessCounter.WithLabelValues(operationName, p.name).Inc()
	return nil
}

func (p *Probe) prepareBucket() error {
	exists, errBucketExists := p.s3Client.BucketExists(p.bucketName)
	if errBucketExists != nil {
		return errBucketExists
	}
	if exists {
		return nil
	}
	err := p.s3Client.MakeBucket(p.bucketName, "")
	lifecycle1d := `<LifecycleConfiguration>
		<Rule>
			<ID>expire-bucket</ID>
			<Prefix></Prefix>
			<Status>Enabled</Status>
			<Expiration>
				<Days>1</Days>
			</Expiration>
		</Rule>
	</LifecycleConfiguration>`
	p.s3Client.SetBucketLifecycle(p.bucketName, lifecycle1d)
	if err != nil {
		return err
	}

	return nil
}

func randomHex(n int) (string, error) {
	buffer := make([]byte, n)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func randomObject(n int64) (io.Reader, error) {
	buffer := make([]byte, n)
	if _, err := rand.Read(buffer); err != nil {
		return bytes.NewReader(buffer), err
	}
	return bytes.NewReader(buffer), nil
}
