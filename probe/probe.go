package probe

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"
	"time"

	"github.com/criteo/s3-probe/config"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/lifecycle"
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
	endpoint                  S3Endpoint
	secretKey                 string
	accessKey                 string
	latencyBucketName         string
	durabilityBucketName      string
	gatewayBucketName         string
	probeRatePerMin           int
	durabilityProbeRatePerMin int
	latencyItemSize           int
	durabilityItemSize        int
	durabilityItemTotal       int
	durabilityTimeout         time.Duration
	latencyTimeout            time.Duration
	gatewayEndpoints          []S3Endpoint
	controlChan               chan bool
}

// S3Endpoint holds the endpoint name address and the client to connect to it
type S3Endpoint struct {
	Name     string
	s3Client *minio.Client
}

// NewProbe creates a new S3 probe
func NewProbe(service S3Service, endpoint string, gatewayEndpoints []S3Endpoint, cfg *config.Config, controlChan chan bool) (Probe, error) {
	minioClient, err := newMinioClientFromEndpoint(endpoint, *cfg.AccessKey, *cfg.SecretKey)
	if err != nil {
		return Probe{}, err
	}

	log.Println("Probe created for:", endpoint)
	return Probe{
		name:                      service.Name,
		gateway:                   service.Gateway,
		endpoint:                  S3Endpoint{Name: endpoint, s3Client: minioClient},
		secretKey:                 *cfg.SecretKey,
		accessKey:                 *cfg.AccessKey,
		latencyBucketName:         *cfg.LatencyBucketName,
		durabilityBucketName:      *cfg.DurabilityBucketName,
		gatewayBucketName:         *cfg.GatewayBucketName,
		probeRatePerMin:           *cfg.ProbeRatePerMin,
		durabilityProbeRatePerMin: *cfg.DurabilityProbeRatePerMin,
		latencyItemSize:           *cfg.LatencyItemSize,
		durabilityItemSize:        *cfg.DurabilityItemSize,
		durabilityItemTotal:       *cfg.DurabilityItemTotal,
		durabilityTimeout:         *cfg.DurabilityTimeout,
		latencyTimeout:            *cfg.LatencyTimeout,
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
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
}

type timer struct {
	C      <-chan time.Time
	Ticker *time.Ticker
}

func newTimer(rate int) timer {
	if rate == 0 {
		fakeTimer := make(chan time.Time)
		return timer{C: fakeTimer, Ticker: nil}
	}
	ticker := time.NewTicker(time.Duration(millisecondInMinute/rate) * time.Millisecond)
	return timer{Ticker: ticker, C: ticker.C}
}

func (t *timer) Stop() {
	if t.Ticker != nil {
		t.Ticker.Stop()
	}
}

func (p *Probe) PrepareProbing() error {
	log.Println("Prepare probing")

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
	return nil
}

// StartProbing start to probe the S3 endpoint
func (p *Probe) StartProbing() error {
	log.Println("Starting probing")

	tickerProbe := newTimer(p.probeRatePerMin)
	tickerDurabilityProbe := newTimer(p.durabilityProbeRatePerMin)

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
	ctx, cancel := context.WithTimeout(context.Background(), p.durabilityTimeout)
	defer cancel()
	s3ExpectedDurabilityItems.WithLabelValues(p.name).Set(float64(p.durabilityItemTotal))
	objectCh := p.endpoint.s3Client.ListObjects(ctx, p.durabilityBucketName, minio.ListObjectsOptions{})
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
	objectSize := int64(p.latencyItemSize)

	operation := func(ctx context.Context) error {
		_, err := p.endpoint.s3Client.ListBuckets(ctx)
		return err
	}
	if err := p.mesureOperation("list_buckets", operation); err != nil {
		return err
	}

	objectData, _ := randomObject(objectSize)
	operation = func(ctx context.Context) error {
		_, err := p.endpoint.s3Client.PutObject(ctx, p.latencyBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		return err
	}
	if err := p.mesureOperation("put_object", operation); err != nil {
		return err
	}

	operation = func(ctx context.Context) error {
		obj, err := p.endpoint.s3Client.GetObject(ctx, p.latencyBucketName, objectName, minio.GetObjectOptions{})
		defer obj.Close()
		data := make([]byte, p.latencyItemSize)
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

	operation = func(ctx context.Context) error {
		err := p.endpoint.s3Client.RemoveObject(ctx, p.latencyBucketName, objectName, minio.RemoveObjectOptions{})
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
	operation := func(ctx context.Context) error {
		_, err := p.endpoint.s3Client.PutObject(ctx, p.gatewayBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		return err
	}
	if err := p.mesureOperation("gateway_put_object", operation); err != nil {
		return err
	}
	var operationName string
	for i := range p.gatewayEndpoints {
		operationName = "gateway_get_object"
		s3GatewayTotalCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].Name).Inc()
		obj, err := p.gatewayEndpoints[i].s3Client.GetObject(context.Background(), p.gatewayBucketName, objectName, minio.GetObjectOptions{})
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
			s3GatewaySuccessCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].Name).Inc()
		}
		obj.Close()

		operationName = "gateway_remove_object"
		s3GatewayTotalCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].Name).Inc()
		err = p.gatewayEndpoints[i].s3Client.RemoveObject(context.Background(), p.gatewayBucketName, objectName, minio.RemoveObjectOptions{})
		if err != nil {
			log.Printf("Error while executing %s: %s", operationName, err)
		} else {
			s3GatewaySuccessCounter.WithLabelValues(operationName, p.name, p.gatewayEndpoints[i].Name).Inc()
		}
	}

	return nil
}

func (p *Probe) mesureOperation(operationName string, operation func(ctx context.Context) error) error {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), p.latencyTimeout)
	defer cancel()
	err := operation(ctx)

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

func (p *Probe) checkDurabilityBucketHasEnoughObject() (bool, error) {
	var countObj = 0
	// Create a done channel to control 'ListObjectsV2' go routine.
	doneCh := make(chan struct{})

	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	objectCh := p.endpoint.s3Client.ListObjects(context.Background(), p.durabilityBucketName, minio.ListObjectsOptions{})
	for object := range objectCh {
		if object.Err != nil {
			return false, object.Err
		}
		countObj++
	}

	if countObj >= p.durabilityItemTotal {
		return true, nil
	}
	return false, nil
}

func (p *Probe) prepareDurabilityBucket() error {
	log.Printf("Checking if durability bucket is present on %s", p.name)
	exists, errBucketExists := p.endpoint.s3Client.BucketExists(context.Background(), p.durabilityBucketName)
	if errBucketExists != nil {
		return errBucketExists
	}

	if exists {
		hasEnoughObjects, err := p.checkDurabilityBucketHasEnoughObject()
		if err != nil {
			return err
		}
		if hasEnoughObjects {
			return nil
		}
	} else {
		err := p.endpoint.s3Client.MakeBucket(context.Background(), p.durabilityBucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
	}

	log.Println("Preparing durability bucket")
	probeBucketAttempt.WithLabelValues(p.name).Inc()
	objectSuffix := "fake-item-"
	objectSize := int64(p.durabilityItemSize)
	objectData, _ := randomObject(objectSize)

	var objectName string
	for i := 0; i < p.durabilityItemTotal; i++ {
		objectName = objectSuffix + strconv.Itoa(i)
		_, err := p.endpoint.s3Client.PutObject(context.Background(), p.durabilityBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})

		for err != nil {
			log.Printf("Error (item: %d): %s, retrying in (5s)", i, err)
			time.Sleep(5 * time.Second)
			_, err = p.endpoint.s3Client.PutObject(context.Background(), p.durabilityBucketName, objectName, objectData, objectSize, minio.PutObjectOptions{})
		}
		if i%100 == 0 {
			log.Printf("%s> %d objects written (%d%%)", p.name, i, int((float64(i)/float64(p.durabilityItemTotal))*100))
		}
	}
	return nil
}

func (p *Probe) prepareLatencyBucket() error {
	log.Printf("Checking if latency bucket is present on %s", p.name)
	exists, errBucketExists := p.endpoint.s3Client.BucketExists(context.Background(), p.latencyBucketName)
	if errBucketExists != nil {
		return errBucketExists
	}
	if exists {
		return nil
	}
	log.Println("Preparing latency bucket")
	probeBucketAttempt.WithLabelValues(p.name).Inc()

	err := p.endpoint.s3Client.MakeBucket(context.Background(), p.latencyBucketName, minio.MakeBucketOptions{})
	if err != nil {
		return err
	}

	setBucketLifecycle1d(p.endpoint.s3Client, p.latencyBucketName)
	return nil
}

func (p *Probe) prepareGatewayBucket() error {
	log.Printf("Checking if gateway buckets are present on %s", p.name)
	if len(p.gatewayEndpoints) == 0 {
		return errors.New("Couldn't find any gateway destinations")
	}
	for i := range p.gatewayEndpoints {
		exists, errBucketExists := p.gatewayEndpoints[i].s3Client.BucketExists(context.Background(), p.gatewayBucketName)
		if errBucketExists != nil {
			return errBucketExists
		}
		if exists {
			continue
		}
		log.Printf("Preparing gateway bucket on %s", p.gatewayEndpoints[i].Name)
		probeGatewayBucketAttempt.WithLabelValues(p.name, p.gatewayEndpoints[i].Name).Inc()

		err := p.gatewayEndpoints[i].s3Client.MakeBucket(context.Background(), p.gatewayBucketName, minio.MakeBucketOptions{})
		if err != nil {
			return err
		}
		setBucketLifecycle1d(p.gatewayEndpoints[i].s3Client, p.gatewayBucketName)
	}
	return nil
}

func setBucketLifecycle1d(client *minio.Client, bucketName string) {
	lc := lifecycle.NewConfiguration()
	lc.Rules = []lifecycle.Rule{
		{
			ID:     "expire-bucket",
			Status: "Enabled",
			Expiration: lifecycle.Expiration{
				Days: 1,
			},
		},
	}
	client.SetBucketLifecycle(context.Background(), bucketName, lc)
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
