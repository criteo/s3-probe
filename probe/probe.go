package probe

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/criteo/s3-probe/config"
	minio "github.com/minio/minio-go/v6"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var s3LatencySummary = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "s3_latency_seconds",
	Help: "Latency for operation on the S3 endpoint",
}, []string{"operation", "endpoint"})

var s3LatencyHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "s3_latency_histogram_seconds",
	Help:    "Latency for operation on the S3 endpoint",
	Buckets: []float64{.001, .0025, .005, .010, .015, .020, .025, .030, .040, .050, .060, .075, .100, .250, .500, 1, 2.5, 5, 10},
}, []string{"operation", "endpoint"})

var s3TotalCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_request_total",
	Help: "Total number of requests on S3 endpoint",
}, []string{"operation", "endpoint"})

var s3SuccessCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_request_success_total",
	Help: "Total number of successful requests on S3 endpoint",
}, []string{"operation", "endpoint"})

var s3GatewayTotalCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_gateway_request_total",
	Help: "Total number of gateway requests on S3 endpoint",
}, []string{"operation", "endpoint", "gateway_endpoint"})

var s3GatewaySuccessCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_gateway_request_success_total",
	Help: "Total number of successful gateway requests on S3 endpoint",
}, []string{"operation", "endpoint", "gateway_endpoint"})

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

var probeGatewayBucketAttempt = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "probe_gateway_bucket_created_total",
	Help: "Total number of monitoring gateway bucket created",
}, []string{"endpoint", "gateway_endpoint"})

const millisecondInMinute = 60_000

// Probe is a S3 probe
type Probe struct {
	name                      string
	gateway                   bool
	endpoint                  s3endpoint
	secretKey                 string
	accessKey                 string
	latencyBucketName         string
	durabilityBucketName      string
	gatewayBucketName         string
	probeRatePerMin           int
	durabilityProbeRatePerMin int
	durabilityItemSize        int
	durabilityItemTotal       int
	gatewayEndpoints          []s3endpoint
	controlChan               chan bool
}

type s3endpoint struct {
	name     string
	s3Client *minio.Client
}

// NewProbe creates a new S3 probe
func NewProbe(service S3Service, endpoint string, gatewayEndpoints []s3endpoint, cfg config.Config, controlChan chan bool) (Probe, error) {
	minioClient, err := newMinioClientFromEndpoint(endpoint, *cfg.AccessKey, *cfg.SecretKey)
	if err != nil {
		return Probe{}, err
	}

	log.Println("Probe created for:", endpoint)
	return Probe{
		name:                      service.Name,
		gateway:                   service.Gateway,
		endpoint:                  s3endpoint{name: endpoint, s3Client: minioClient},
		secretKey:                 *cfg.SecretKey,
		accessKey:                 *cfg.AccessKey,
		latencyBucketName:         *cfg.LatencyBucketName,
		durabilityBucketName:      *cfg.DurabilityBucketName,
		gatewayBucketName:         *cfg.GatewayBucketName,
		probeRatePerMin:           *cfg.ProbeRatePerMin,
		durabilityProbeRatePerMin: *cfg.DurabilityProbeRatePerMin,
		durabilityItemSize:        *cfg.DurabilityItemSize,
		durabilityItemTotal:       *cfg.DurabilityItemTotal,
		controlChan:               controlChan,
		gatewayEndpoints:          gatewayEndpoints,
	}, nil
}

func newMinioClientFromEndpoint(endpoint string, accessKey string, secretKey string) (*minio.Client, error) {
	re := regexp.MustCompile("^(http[s]+://)?(.*)")
	match := re.FindStringSubmatch(endpoint)
	secure := false
	if match[1] == "https://" {
		endpoint = match[2]
		secure = true
	} else if match[1] == "http://" {
		endpoint = match[2]
	}

	return minio.New(endpoint, accessKey, secretKey, secure)
}

// StartProbing start to probe the S3 endpoint
func (p *Probe) StartProbing() error {
	log.Println("Starting probing")

	if p.gateway {
		err := p.prepareGatewayBucket()
		if err != nil {
			log.Println("Error: cannot prepare gateway latency bucket:", err)
			return err
		}
	} else {
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
	}

	var tickerProbe, tickerDurabilityProbe *time.Ticker
	if p.probeRatePerMin == 0 {
		tickerProbe = time.NewTicker(time.Hour * 999999)
	} else {
		tickerProbe = time.NewTicker(time.Duration(millisecondInMinute/p.probeRatePerMin) * time.Millisecond)
	}
	if p.durabilityProbeRatePerMin == 0 {
		tickerDurabilityProbe = time.NewTicker(time.Hour * 999999)
	} else {
		tickerDurabilityProbe = time.NewTicker(time.Duration(millisecondInMinute/p.durabilityProbeRatePerMin) * time.Millisecond)
	}

	for {
		select {
		// If we receive something on the control chan we terminate
		// otherwise we continue to perform checks
		case <-p.controlChan:
			log.Println("Terminating probe on", p.name)
			tickerProbe.Stop()
			tickerDurabilityProbe.Stop()
			return nil
		case <-tickerProbe.C:
			if p.gateway {
				go p.performGatewayChecks()
			} else {
				go p.performLatencyChecks()
			}
		case <-tickerDurabilityProbe.C:
			if !p.gateway {
				go p.performDurabilityChecks()
			}
		}
	}
}

func (p *Probe) performDurabilityChecks() error {
	doneCh := make(chan struct{})
	s3ExpectedDurabilityItems.WithLabelValues(p.name).Set(float64(p.durabilityItemTotal))
	objectCh := p.endpoint.s3Client.ListObjects(p.durabilityBucketName, "", false, doneCh)
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
		_, err := p.endpoint.s3Client.ListBuckets()
		return err
	}
	if err := p.mesureOperation("list_buckets", operation); err != nil {
		return err
	}

	objectData, _ := randomObject(objectSize)
	operation = func() error {
		_, err := p.endpoint.s3Client.PutObject(p.latencyBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		return err
	}
	if err := p.mesureOperation("put_object", operation); err != nil {
		return err
	}

	operation = func() error {
		obj, err := p.endpoint.s3Client.GetObject(p.latencyBucketName, objectName, minio.GetObjectOptions{})
		defer obj.Close()
		// Read data by chunks of 1024 bytes
		data := make([]byte, 1024)
		for {
			_, err = obj.Read(data)
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
	}
	if err := p.mesureOperation("get_object", operation); err != nil {
		return err
	}

	operation = func() error {
		err := p.endpoint.s3Client.RemoveObject(p.latencyBucketName, objectName)
		return err
	}
	if err := p.mesureOperation("remove_object", operation); err != nil {
		return err
	}

	return nil
}

func (p *Probe) performGatewayChecks() error {
	objectName, _ := randomHex(20)
	objectSize := int64(1024)

	objectData, _ := randomObject(objectSize)
	operation := func() error {
		_, err := p.endpoint.s3Client.PutObject(p.gatewayBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		return err
	}
	if err := p.mesureOperation("gateway_put_object", operation); err != nil {
		return err
	}
	var operationName string
	for i := range p.gatewayEndpoints {
		operationName = "gateway_get_object"
		s3GatewayTotalCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].name).Inc()
		obj, err := p.gatewayEndpoints[i].s3Client.GetObject(p.gatewayBucketName, objectName, minio.GetObjectOptions{})
		if err != nil {
			log.Printf("Error while executing %s: %s", operationName, err)
		}
		// Read data by chunks of 1024 bytes
		data := make([]byte, 1024)
		for _, err = obj.Read(data); err == nil; {
		}
		if err != io.EOF {
			log.Printf("Error while executing %s: %s", operationName, err)
		} else {
			s3GatewaySuccessCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].name).Inc()
		}
		obj.Close()

		operationName = "gateway_remove_object"
		s3GatewayTotalCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].name).Inc()
		err = p.gatewayEndpoints[i].s3Client.RemoveObject(p.gatewayBucketName, objectName)
		if err != nil {
			log.Printf("Error while executing %s: %s", operationName, err)
		} else {
			s3GatewaySuccessCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].name).Inc()
		}
	}

	return nil
}

func (p *Probe) mesureOperation(operationName string, operation func() error) error {
	start := time.Now()
	err := operation()

	s3TotalCounter.WithLabelValues(operationName, p.name).Inc()
	s3LatencyHistogram.WithLabelValues(operationName, p.name).Observe(time.Since(start).Seconds())
	s3LatencySummary.WithLabelValues(operationName, p.name).Observe(time.Since(start).Seconds())

	if err != nil {
		log.Printf("Error while executing %s: %s", operationName, err)
		return err
	}
	s3SuccessCounter.WithLabelValues(operationName, p.name).Inc()
	return nil
}

func (p *Probe) prepareDurabilityBucket() error {
	log.Printf("Checking if durability bucket is present on %s", p.name)
	exists, errBucketExists := p.endpoint.s3Client.BucketExists(p.durabilityBucketName)
	if errBucketExists != nil {
		return errBucketExists
	}
	if exists {
		return nil
	}
	err := p.endpoint.s3Client.MakeBucket(p.durabilityBucketName, "")
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
		_, err := p.endpoint.s3Client.PutObject(p.durabilityBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})

		for err != nil {
			log.Printf("Error (item: %d): %s, retrying in (5s)", i, err)
			time.Sleep(5 * time.Second)
			_, err = p.endpoint.s3Client.PutObject(p.durabilityBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		}
		if i%100 == 0 {
			log.Printf("%s> %d objects written (%d%%)", p.name, i, int((float64(i)/float64(p.durabilityItemTotal))*100))
		}
	}
	return nil
}

func (p *Probe) prepareLatencyBucket() error {
	log.Printf("Checking if latency bucket is present on %s", p.name)
	exists, errBucketExists := p.endpoint.s3Client.BucketExists(p.latencyBucketName)
	if errBucketExists != nil {
		return errBucketExists
	}
	if exists {
		return nil
	}
	log.Println("Preparing latency bucket")
	probeBucketAttempt.WithLabelValues(p.name).Inc()

	err := p.endpoint.s3Client.MakeBucket(p.latencyBucketName, "")
	if err != nil {
		return err
	}

	setBucketLifecycle1d(p.endpoint.s3Client, p.latencyBucketName)
	return nil
}

func (p *Probe) prepareGatewayBucket() error {
	log.Printf("Checking if gateway buckets are present on %s", p.name)
	if len(p.gatewayEndpoints) == 0 {
		return errors.New("Couldn't find any geteway destinations")
	}
	for i := range p.gatewayEndpoints {
		exists, errBucketExists := p.gatewayEndpoints[i].s3Client.BucketExists(p.gatewayBucketName)
		if errBucketExists != nil {
			return errBucketExists
		}
		if exists {
			return nil
		}
		log.Printf("Preparing gateway bucket on %s", p.gatewayEndpoints[i].name)
		probeGatewayBucketAttempt.WithLabelValues(p.name, p.gatewayEndpoints[i].name).Inc()

		err := p.gatewayEndpoints[i].s3Client.MakeBucket(p.gatewayBucketName, "")
		if err != nil {
			return err
		}
		setBucketLifecycle1d(p.gatewayEndpoints[i].s3Client, p.gatewayBucketName)
	}
	return nil
}

func setBucketLifecycle1d(client *minio.Client, bucketName string) {
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

	client.SetBucketLifecycle(bucketName, lifecycle1d)
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
