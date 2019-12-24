package watcher

import (
	"log"
	"time"

	"github.com/criteo/s3-probe/probe"

	"github.com/criteo/s3-probe/config"

	consul_api "github.com/hashicorp/consul/api"
)

type s3Service struct {
	name        string
	serviceType string
}

// Watcher manages the pool of S3 endpoints to monitor
type Watcher struct {
	consulClient *consul_api.Client
	cfg          config.Config
	consulTag    string
	s3Pools      map[string](chan bool)
}

// NewWatcher creates a new watcher and prepare the consul client
func NewWatcher(cfg config.Config) Watcher {
	defaultConfig := consul_api.DefaultConfig()
	defaultConfig.Address = *cfg.ConsulAddr
	client, err := consul_api.NewClient(defaultConfig)
	if err != nil {
		panic(err)
	}
	return Watcher{
		cfg:          cfg,
		consulClient: client,
		consulTag:    *cfg.Tag,
		s3Pools:      make(map[string](chan bool)),
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

func (w *Watcher) createNewProbes(servicesToAdd []s3Service) {
	var probeChan chan bool
	for i := range servicesToAdd {
		log.Printf("Creating new probe for: %s", servicesToAdd[i])
		probeChan = make(chan bool)
		w.s3Pools[servicesToAdd[i].name] = probeChan
		p, err := probe.NewProbe(servicesToAdd[i].name, w.cfg, probeChan)
		if err != nil {
			log.Println("Error while creating probe:", err)
		}
		go p.StartProbing()
	}
}

func (w *Watcher) flushOldProbes(servicesToRemove []s3Service) {
	var ok bool
	var probeChan chan bool
	for i := range servicesToRemove {
		log.Printf("Removing old probe for: %s", servicesToRemove[i])
		probeChan, ok = w.s3Pools[servicesToRemove[i].name]
		if ok {
			probeChan <- false
			close(probeChan)
		}
	}
}

// getServicesToModify compare services as seen in consul and services that are running in the probe. Every service that
// Are in consul and not on the probe are added to the probe. Services in the probe that are not in consul are removed
func (w *Watcher) getServicesToModify(servicesFromConsul []s3Service, watchedServices []s3Service) ([]s3Service, []s3Service) {
	servicesToAdd := getSliceDiff(watchedServices, servicesFromConsul)
	servicesToRemove := getSliceDiff(servicesFromConsul, watchedServices)
	return servicesToAdd, servicesToRemove
}

func (w *Watcher) getWatchedServices() []s3Service {
	currentServices := []s3Service{}

	for k := range w.s3Pools {
		currentServices = append(currentServices, s3Service{name: k})
	}
	return currentServices
}

func (w *Watcher) getServices() []s3Service {
	catalog := w.consulClient.Catalog()
	services, _, _ := catalog.Services(nil)

	var s3Services []s3Service
	var service s3Service
	for serviceName := range services {
		for i := range services[serviceName] {
			if services[serviceName][i] == w.consulTag {
				service = s3Service{name: serviceName, serviceType: "simple"}
				s3Services = append(s3Services, service)
				break
			}
		}
	}
	return s3Services
}

// getDiff return the elements from mainSlice that are not in subSlice
func getSliceDiff(mainSlice []s3Service, subSlice []s3Service) []s3Service {
	mainIndex := make(map[string]bool)
	var result []s3Service
	for i := range mainSlice {
		mainIndex[mainSlice[i].name] = true
	}
	for i := range subSlice {
		if !mainIndex[subSlice[i].name] {
			result = append(result, subSlice[i])
		}
	}
	return result
}
