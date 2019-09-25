package watcher

import (
	"log"
	"time"

	"github.com/s3-probe/probe"

	consul_api "github.com/hashicorp/consul/api"
)

// Watcher manages the pool of S3 endpoints to monitor
type Watcher struct {
	consulClient         *consul_api.Client
	consulTag            string
	endpointSuffix       string
	latencyBucketName    string
	durabilityBucketName string
	durabilityItemSize   int
	durabilityItemTotal  int
	accessKey            string
	secretKey            string
	probeRatePerMin      int
	s3Pools              map[string](chan bool)
}

// NewWatcher creates a new watcher and prepare the consul client
func NewWatcher(consulAddr string, consulTag string, suffix string, latencyBucketName string, durabilityBucketName string, durabilityItemSize int, durabilityItemTotal int, accessKey string, secretKey string, probeRatePerMin int) Watcher {
	defaultConfig := consul_api.DefaultConfig()
	defaultConfig.Address = consulAddr
	client, err := consul_api.NewClient(defaultConfig)
	if err != nil {
		panic(err)
	}
	return Watcher{
		consulClient:         client,
		consulTag:            consulTag,
		endpointSuffix:       suffix,
		latencyBucketName:    latencyBucketName,
		durabilityBucketName: durabilityBucketName,
		durabilityItemSize:   durabilityItemSize,
		durabilityItemTotal:  durabilityItemTotal,
		accessKey:            accessKey,
		secretKey:            secretKey,
		probeRatePerMin:      probeRatePerMin,
		s3Pools:              make(map[string](chan bool)),
	}
}

// WatchPools poll consul services with specified tag and create
// probe gorountines
func (w *Watcher) WatchPools(interval time.Duration) {
	for {
		log.Printf("Discovering S3 endpoints (interval: %s)", interval)
		servicesFromConsul := w.getServices()
		watchedServices := w.getWatchedServices()
		servicesToAdd, servicesToRemove := w.getServicesToModify(servicesFromConsul, watchedServices)
		w.flushOldProbes(servicesToRemove)
		w.createNewProbes(servicesToAdd)
		time.Sleep(interval)
	}

}

func (w *Watcher) createNewProbes(servicesToAdd []string) {
	var probeChan chan bool
	for i := range servicesToAdd {
		log.Printf("Creating new probe for: %s", servicesToAdd[i])
		probeChan = make(chan bool)
		w.s3Pools[servicesToAdd[i]] = probeChan
		p, err := probe.NewProbe(servicesToAdd[i], w.endpointSuffix, w.accessKey, w.secretKey, w.latencyBucketName, w.durabilityBucketName, w.probeRatePerMin, w.durabilityItemSize, w.durabilityItemTotal, probeChan)
		if err != nil {
			log.Println("Error while creating probe:", err)
		}
		go p.StartProbing()
	}
}

func (w *Watcher) flushOldProbes(servicesToRemove []string) {
	var ok bool
	var probeChan chan bool
	for i := range servicesToRemove {
		log.Printf("Removing old probe for: %s", servicesToRemove[i])
		probeChan, ok = w.s3Pools[servicesToRemove[i]]
		if ok {
			probeChan <- false
			close(probeChan)
		}
	}
}

// getServicesToModify compare services as seen in consul and services that are running in the probe. Every service that
// Are in consul and not on the probe are added to the probe. Services in the probe that are not in consul are removed
func (w *Watcher) getServicesToModify(servicesFromConsul []string, watchedServices []string) ([]string, []string) {
	servicesToAdd := getSliceDiff(watchedServices, servicesFromConsul)
	servicesToRemove := getSliceDiff(servicesFromConsul, watchedServices)
	return servicesToAdd, servicesToRemove
}

func (w *Watcher) getWatchedServices() []string {
	currentServices := []string{}

	for k := range w.s3Pools {
		currentServices = append(currentServices, k)
	}
	return currentServices
}

func (w *Watcher) getServices() []string {
	catalog := w.consulClient.Catalog()
	services, _, _ := catalog.Services(nil)

	var s3Services []string
	for service := range services {
		for i := range services[service] {
			if services[service][i] == w.consulTag {
				s3Services = append(s3Services, service)
				break
			}
		}
	}
	return s3Services
}

// getDiff return the elements from mainSlice that are not in subSlice
func getSliceDiff(mainSlice []string, subSlice []string) []string {
	mainIndex := make(map[string]bool)
	var result []string
	for i := range mainSlice {
		mainIndex[mainSlice[i]] = true
	}
	for i := range subSlice {
		if !mainIndex[subSlice[i]] {
			result = append(result, subSlice[i])
		}
	}
	return result
}
