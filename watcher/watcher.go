package watcher

import (
	"log"
	"time"

	"github.com/criteo/s3-probe/probe"

	"github.com/criteo/s3-probe/config"

	consul_api "github.com/hashicorp/consul/api"
)

// Watcher manages the pool of S3 endpoints to monitor
type Watcher struct {
	consulClient     *consul_api.Client
	cfg              config.Config
	consulTag        string
	consulGatewayTag string
	s3Pools          map[string](chan bool)
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
		cfg:              cfg,
		consulClient:     client,
		consulTag:        *cfg.Tag,
		consulGatewayTag: *cfg.GatewayTag,
		s3Pools:          make(map[string](chan bool)),
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
	var probeChan chan bool
	for i := range servicesToAdd {
		log.Printf("Creating new probe for: %s, gateway: %t", servicesToAdd[i].Name, servicesToAdd[i].Gateway)
		probeChan = make(chan bool)
		p, err := probe.NewProbeFromConsul(servicesToAdd[i], w.cfg, *w.consulClient, probeChan)

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

		w.s3Pools[servicesToAdd[i].Name] = probeChan
		go p.StartProbing()
	}
}

func (w *Watcher) flushOldProbes(servicesToRemove []probe.S3Service) {
	var ok bool
	var probeChan chan bool
	for i := range servicesToRemove {
		log.Printf("Removing old probe for: %s", servicesToRemove[i].Name)
		probeChan, ok = w.s3Pools[servicesToRemove[i].Name]
		if ok {
			delete(w.s3Pools, servicesToRemove[i].Name);
			probeChan <- false
			close(probeChan)
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

	for k := range w.s3Pools {
		currentServices = append(currentServices, probe.S3Service{Name: k})
	}
	return currentServices
}

func (w *Watcher) getServices() []probe.S3Service {
	catalog := w.consulClient.Catalog()
	services, _, _ := catalog.Services(nil)

	var s3Services []probe.S3Service
	var service probe.S3Service
	for serviceName := range services {
		for i := range services[serviceName] {
			if services[serviceName][i] == w.consulGatewayTag {
				service = probe.S3Service{Name: serviceName, Gateway: true}
				s3Services = append(s3Services, service)
				break
			}
			if services[serviceName][i] == w.consulTag {
				service = probe.S3Service{Name: serviceName, Gateway: false}
				s3Services = append(s3Services, service)
				break
			}
		}
	}
	return s3Services
}

// getDiff return the elements from mainSlice that are not in subSlice
func getSliceDiff(mainSlice []probe.S3Service, subSlice []probe.S3Service) []probe.S3Service {
	mainIndex := make(map[string]bool)
	var result []probe.S3Service
	for i := range mainSlice {
		mainIndex[mainSlice[i].Name] = true
	}
	for i := range subSlice {
		if !mainIndex[subSlice[i].Name] {
			result = append(result, subSlice[i])
		}
	}
	return result
}
