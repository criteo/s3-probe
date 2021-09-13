package watcher

import (
	"github.com/criteo/s3-probe/pkg/config"
	"github.com/criteo/s3-probe/pkg/probe"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type watchedService struct {
	service   probe.S3Service
	probeChan chan bool
}

// Watcher manages the pool of S3 endpoints to monitor
type Watcher struct {
	consulClient    probe.ConsulClient
	cfg             *config.Config
	watchedServices map[string]watchedService
}

var serviceDiscoveryErrorCounter = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "s3_service_discovery_error_total",
	Help: "Total number of service errors",
}, []string{"service"})

// NewWatcher creates a new watcher and prepare the consul client
func NewWatcher(cfg config.Config) Watcher {
	client, err := probe.MakeConsulClient(&cfg)
	if err != nil {
		panic(err)
	}
	return Watcher{
		cfg:             &cfg,
		consulClient:    client,
		watchedServices: map[string]watchedService{},
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

func (w *Watcher) createNewProbes(servicesToAdd []probe.S3Service) {
	for _, s3service := range servicesToAdd {
		log.Printf("Creating new probe for: %s, gateway: %t", s3service.Name, s3service.Gateway)
		probeChan := make(chan bool)

		p, err := probe.NewProbeFromConsul(s3service, w.cfg, probeChan)
		if err != nil {
			log.Println("Error while creating probe:", err)
			continue
		}

		err = p.PrepareProbing()
		if err != nil {
			log.Println("Error while preparing probe:", err)
			close(probeChan)
			continue
		}

		w.watchedServices[s3service.Name] = watchedService{service: s3service, probeChan: probeChan}
		go p.StartProbing()
	}
}

func (w *Watcher) flushOldProbes(servicesToRemove []probe.S3Service) {
	for _, s3service := range servicesToRemove {
		log.Printf("Removing old probe for: %s", s3service.Name)
		ws, ok := w.watchedServices[s3service.Name]
		if ok {
			delete(w.watchedServices, s3service.Name)
			ws.probeChan <- false
			close(ws.probeChan)
		}
	}
}

// getServicesToModify compare services as seen in consul and services that are running in the probe. Every service that
// Are in consul and not on the probe are added to the probe. Services in the probe that are not in consul are removed
func (w *Watcher) getServicesToModify(servicesFromConsul []probe.S3Service, watchedServices []probe.S3Service) ([]probe.S3Service, []probe.S3Service) {
	servicesToAdd := getSliceDiff(watchedServices, servicesFromConsul)
	servicesToRemove := getSliceDiff(servicesFromConsul, watchedServices)
	return servicesToAdd, servicesToRemove
}

func (w *Watcher) getWatchedServices() []probe.S3Service {
	currentServices := []probe.S3Service{}

	for _, ws := range w.watchedServices {
		currentServices = append(currentServices, ws.service)
	}
	return currentServices
}

func (w *Watcher) getServices() []probe.S3Service {
	services, err := w.consulClient.GetAllMatchingRegisteredServices()
	if err != nil {
		serviceDiscoveryErrorCounter.WithLabelValues("N/A").Inc()
		log.Printf("Fail to query all registered services from consul: %s\n", err)
		return []probe.S3Service{}
	}

	results := make([]probe.S3Service, 0)
	for serviceName, isGateway := range services {
		endpoint, readEndpoints, err := w.consulClient.GetServiceEndPoints(serviceName, isGateway)
		if err != nil {
			serviceDiscoveryErrorCounter.WithLabelValues(serviceName).Inc()
			log.Printf("Resolving service endpoints failed for %s: %s\n", serviceName, err)
			continue
		}

		s := probe.S3Service{Name: serviceName, Endpoint: endpoint, Gateway: isGateway, GatewayReadEnpoints: readEndpoints}
		results = append(results, s)
	}

	return results
}

// getDiff return the elements from mainSlice that are not in subSlice or that have differences
func getSliceDiff(mainSlice []probe.S3Service, subSlice []probe.S3Service) []probe.S3Service {
	mainIndex := make(map[string]*probe.S3Service)
	var result []probe.S3Service
	for i := range mainSlice {
		mainIndex[mainSlice[i].Name] = &mainSlice[i]
	}
	for i := range subSlice {
		service, found := mainIndex[subSlice[i].Name]
		if !found || !service.Equals(&subSlice[i]) {
			result = append(result, subSlice[i])
		}
	}
	return result
}
