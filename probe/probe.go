package probe

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log"
	"strconv"
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

var s3ExpectedDurabilityItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "s3_durability_items_expected",
	Help: "Number of items that should be present on the endpoint",
}, []string{"endpoint"})

var s3FoundDurabilityItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "s3_durability_items_found",
	Help: "Number of items that are present on the endpoint",
}, []string{"endpoint"})

var probeBucketAttempt = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "probe_bucket_created_total",
	Help: "Total number of monitoring bucket created",
}, []string{"endpoint"})

const millisecondInMinute = 60_000

// Probe is a S3 probe
type Probe struct {
	name                 string
	endpoint             string
	secretKey            string
	accessKey            string
	latencyBucketName    string
	durabilityBucketName string
	probeRatePerMin      int
	durabilityItemSize   int
	durabilityItemTotal  int
	s3Client             *minio.Client
	controlChan          chan bool
}

// NewProbe creates a new S3 probe
func NewProbe(name string, suffix string, accessKey string, secretKey string, latencyBucketName string, durabilityBucketName string, probeRatePerMin int, durabilityItemSize int, durabilityItemTotal int, controlChan chan bool) (Probe, error) {
	endpoint := name + suffix
	minioClient, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		return Probe{}, err
	}

	log.Println("Probe created for:", endpoint)
	return Probe{
		name:                 name,
		endpoint:             endpoint,
		secretKey:            secretKey,
		accessKey:            accessKey,
		latencyBucketName:    latencyBucketName,
		durabilityBucketName: durabilityBucketName,
		probeRatePerMin:      probeRatePerMin,
		durabilityItemSize:   durabilityItemSize,
		durabilityItemTotal:  durabilityItemTotal,
		controlChan:          controlChan,
		s3Client:             minioClient,
	}, nil
}

// StartProbing start to probe the S3 endpoint
func (p *Probe) StartProbing() error {
	log.Println("Starting probing")
	for {
		select {
		// If we receive something on the control chan we terminate
		// otherwise we continue to perform checks
		case <-p.controlChan:
			log.Println("Terminating probe on", p.name)
			return nil
		case <-time.After(time.Duration(millisecondInMinute/p.probeRatePerMin) * time.Millisecond):
			err := p.prepareLatencyBucket()
			if err != nil {
				log.Println("Error: cannot prepare latency bucket:", err)
				return err
			}
			err = p.prepareDurabilityBucket()
			if err != nil {
				log.Println("Error: cannot prepare durability bucket:", err)
				return err
			}
			go p.performLatencyChecks()
			go p.performDurabilityChecks()
		}
	}
}

func (p *Probe) performDurabilityChecks() error {
	doneCh := make(chan struct{})
	s3ExpectedDurabilityItems.WithLabelValues(p.name).Set(float64(p.durabilityItemTotal))
	objectCh := p.s3Client.ListObjects(p.durabilityBucketName, "", false, doneCh)
	objectTotal := 0
	for object := range objectCh {
		if object.Err != nil {
			log.Println(object.Err)
			return object.Err
		}
		objectTotal++
	}
	s3FoundDurabilityItems.WithLabelValues(p.name).Set(float64(objectTotal))
	return nil
}

func (p *Probe) performLatencyChecks() error {
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
		_, err := p.s3Client.PutObject(p.latencyBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		return err
	}
	if err := p.mesureOperation("put_object", operation); err != nil {
		return err
	}

	operation = func() error {
		_, err := p.s3Client.GetObject(p.latencyBucketName, objectName, minio.GetObjectOptions{})
		return err
	}
	if err := p.mesureOperation("get_object", operation); err != nil {
		return err
	}

	operation = func() error {
		err := p.s3Client.RemoveObject(p.latencyBucketName, objectName)
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

func (p *Probe) prepareDurabilityBucket() error {
	exists, errBucketExists := p.s3Client.BucketExists(p.durabilityBucketName)
	if errBucketExists != nil {
		return errBucketExists
	}
	if exists {
		return nil
	}
	err := p.s3Client.MakeBucket(p.durabilityBucketName, "")
	if err != nil {
		return err
	}

	log.Println("Preparing durability bucket")
	probeBucketAttempt.WithLabelValues(p.name).Inc()
	objectSuffix := "fake-item-"
	objectSize := int64(p.durabilityItemSize)
	objectData, _ := randomObject(objectSize)

	var objectName string
	for i := 0; i < p.durabilityItemTotal; i++ {
		objectName = objectSuffix + strconv.Itoa(i)
		_, err := p.s3Client.PutObject(p.durabilityBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})

		for err != nil {
			log.Printf("Error (item: %d): %s, retrying in (5s)", i, err)
			time.Sleep(5 * time.Second)
			_, err = p.s3Client.PutObject(p.durabilityBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		}
		if i%100 == 0 {
			log.Printf("%s> %d objects written (%d%%)", p.name, i, int((float64(i)/float64(p.durabilityItemTotal))*100))
		}
	}
	return nil
}

func (p *Probe) prepareLatencyBucket() error {
	exists, errBucketExists := p.s3Client.BucketExists(p.latencyBucketName)
	if errBucketExists != nil {
		return errBucketExists
	}
	if exists {
		return nil
	}
	log.Println("Preparing latency bucket")
	probeBucketAttempt.WithLabelValues(p.name).Inc()

	err := p.s3Client.MakeBucket(p.latencyBucketName, "")
	if err != nil {
		return err
	}

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

	p.s3Client.SetBucketLifecycle(p.latencyBucketName, lifecycle1d)
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
