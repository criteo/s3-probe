package watcher

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/smartystreets/assertions/assert"
	"github.com/smartystreets/assertions/should"

	"github.com/criteo/s3-probe/config"
	"github.com/criteo/s3-probe/probe"

	io_prometheus_client "github.com/prometheus/client_model/go"
)

type consulClientMock struct {
	RegisteredServices      map[string]bool
	RegisteredServicesError error
	ServiceEndPoints        map[string]string
	ReadEndPoints           map[string][]probe.S3Endpoint
	ServiceEndPointsError   error
}

func (cc *consulClientMock) GetAllMatchingRegisteredServices() (map[string]bool, error) {
	if cc.RegisteredServicesError != nil {
		return map[string]bool{}, cc.RegisteredServicesError
	}
	return cc.RegisteredServices, nil
}

func (cc *consulClientMock) GetServiceEndPoints(serviceName string, isGateway bool) (string, []probe.S3Endpoint, error) {
	if cc.ServiceEndPointsError != nil {
		return "", []probe.S3Endpoint{}, cc.ServiceEndPointsError
	}
	return cc.ServiceEndPoints[serviceName], cc.ReadEndPoints[serviceName], nil
}

func TestGetServiceFailureToListServices(t *testing.T) {
	consulClient := &consulClientMock{}
	consulClient.RegisteredServicesError = errors.New("failure")

	cfg := config.GetTestConfig()
	watcher := Watcher{consulClient: consulClient, cfg: &cfg, watchedServices: map[string]watchedService{}}

	serviceDiscoveryErrorCounter.Reset()
	services := watcher.getServices()
	if len(services) != 0 {
		t.Error("GetServices should have failed")
	}

	m, _ := serviceDiscoveryErrorCounter.GetMetricWithLabelValues("N/A")
	metric := &io_prometheus_client.Metric{}
	m.Write(metric)
	if *metric.Counter.Value != 1.0 {
		t.Errorf("Expected 1.0 got %f", *metric.Counter.Value)
	}
}

func TestGetServiceFailureToGetEndPoints(t *testing.T) {
	consulClient := &consulClientMock{}
	consulClient.RegisteredServices = map[string]bool{"myservice": false, "myotherservice": true}
	consulClient.ServiceEndPointsError = errors.New("failure")

	cfg := config.GetTestConfig()
	watcher := Watcher{consulClient: consulClient, cfg: &cfg, watchedServices: map[string]watchedService{}}

	serviceDiscoveryErrorCounter.Reset()
	services := watcher.getServices()
	if len(services) != 0 {
		t.Error("GetServices should have failed")
	}

	m, _ := serviceDiscoveryErrorCounter.GetMetricWithLabelValues("myservice")
	metric := &io_prometheus_client.Metric{}
	m.Write(metric)
	if *metric.Counter.Value != 1.0 {
		t.Errorf("Expected 1.0 got %f", *metric.Counter.Value)
	}

	m, _ = serviceDiscoveryErrorCounter.GetMetricWithLabelValues("myotherservice")
	metric = &io_prometheus_client.Metric{}
	m.Write(metric)
	if *metric.Counter.Value != 1.0 {
		t.Errorf("Expected 1.0 got %f", *metric.Counter.Value)
	}
}

func TestGetService(t *testing.T) {
	consulClient := &consulClientMock{}
	consulClient.RegisteredServices = map[string]bool{"myservice": false, "myotherservice": true}
	consulClient.ServiceEndPoints = map[string]string{"myservice": "127.0.0.1", "myotherservice": "127.0.0.2"}
	consulClient.ReadEndPoints = map[string][]probe.S3Endpoint{"myotherservice": {probe.S3Endpoint{Name: "10.0.0.1"}, probe.S3Endpoint{Name: "10.0.0.2"}}}

	cfg := config.GetTestConfig()
	watcher := Watcher{consulClient: consulClient, cfg: &cfg, watchedServices: map[string]watchedService{}}

	serviceDiscoveryErrorCounter.Reset()
	services := watcher.getServices()
	if len(services) != 2 {
		t.Errorf("Expected 2 S3Service but got %d", len(services))
	}

	if services[0].Name != "myservice" || services[0].Endpoint != "127.0.0.1" ||
		services[0].Gateway != false || len(services[0].GatewayReadEnpoints) != 0 {
		t.Errorf("myservice don't match expectation")
	}

	if services[1].Name != "myotherservice" || services[1].Endpoint != "127.0.0.2" ||
		services[1].Gateway != true || len(services[1].GatewayReadEnpoints) != 2 {
		t.Errorf("myotherservice don't match expectation")
	}

	m, _ := serviceDiscoveryErrorCounter.GetMetricWithLabelValues("myservice")
	metric := &io_prometheus_client.Metric{}
	m.Write(metric)
	if *metric.Counter.Value != 0.0 {
		t.Errorf("Expected 0.0 got %f", *metric.Counter.Value)
	}

	m, _ = serviceDiscoveryErrorCounter.GetMetricWithLabelValues("myotherservice")
	metric = &io_prometheus_client.Metric{}
	m.Write(metric)
	if *metric.Counter.Value != 0.0 {
		t.Errorf("Expected 0.0 got %f", *metric.Counter.Value)
	}
}

func s3ServicesFromStrings(strings []string) (s3Services []probe.S3Service) {
	for i := range strings {
		s3Services = append(s3Services, probe.S3Service{Name: strings[i]})
	}
	return s3Services
}

func TestGetWatchedServicesReturnTheCorrectEntries(t *testing.T) {
	testValues := []string{"test1", "test2", "test3"}
	testServices := s3ServicesFromStrings(testValues)
	watchedServices := map[string]watchedService{}
	for _, serviceName := range testValues {
		watchedServices[serviceName] = watchedService{service: probe.S3Service{Name: serviceName}}
	}
	w := Watcher{
		watchedServices: watchedServices,
	}
	services := w.getWatchedServices()
	sort.SliceStable(services, func(i, j int) bool {
		return services[i].Name < services[j].Name
	})
	fmt.Println(testServices, services)
	if !reflect.DeepEqual(testServices, services) {
		t.Errorf("Returned value doesn't match given map")
	}
}

func TestGetServicesToModifyHandleLastFromConsul(t *testing.T) {
	servicesFromConsul := s3ServicesFromStrings([]string{"service1", "service2", "service5"})
	servicesWatchedServices := s3ServicesFromStrings([]string{"service2", "service3", "service4"})
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	expectedToAdd := s3ServicesFromStrings([]string{"service1", "service5"})
	expectedToRemove := s3ServicesFromStrings([]string{"service3", "service4"})
	if !reflect.DeepEqual(expectedToAdd, serviceToAdd) {
		fmt.Println(expectedToAdd, serviceToAdd)
		t.Errorf("return values to add are not the one expected")
	}
	if !reflect.DeepEqual(expectedToRemove, serviceToRemove) {
		fmt.Println(expectedToRemove, serviceToRemove)
		t.Errorf("return values to remove are not the one expected")
	}
}

func TestGetServicesToModifyHandleLastFromWatched(t *testing.T) {
	servicesFromConsul := s3ServicesFromStrings([]string{"service1", "service2", "service5"})
	servicesWatchedServices := s3ServicesFromStrings([]string{"service0", "service3", "service5", "service6"})
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	expectedToAdd := s3ServicesFromStrings([]string{"service1", "service2"})
	expectedToRemove := s3ServicesFromStrings([]string{"service0", "service3", "service6"})
	if !reflect.DeepEqual(expectedToAdd, serviceToAdd) {
		fmt.Println(expectedToAdd, serviceToAdd)
		t.Errorf("return values to add are not the one expected")
	}
	if !reflect.DeepEqual(expectedToRemove, serviceToRemove) {
		fmt.Println(expectedToRemove, serviceToRemove)
		t.Errorf("return values to remove are not the one expected")
	}
}

func TestGetServicesToModifyHandleChangeOfEndpoint(t *testing.T) {
	servicesFromConsul := []probe.S3Service{{Name: "s1", Endpoint: "10.0.0.1"}}
	servicesWatchedServices := []probe.S3Service{{Name: "s1", Endpoint: "10.0.0.2"}}
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	if len(serviceToAdd) != 1 || len(serviceToRemove) != 1 {
		t.Errorf("getServicesToModify should have return s1 service in both serviceToRemove and serviceToAdd")
	}
}

func TestGetServicesToModifyHandleChangeOfGatewayReadEndpoint(t *testing.T) {
	servicesFromConsul := []probe.S3Service{{Name: "s1", Endpoint: "10.0.0.1", GatewayReadEnpoints: []probe.S3Endpoint{{Name: "10.0.0.2"}, {Name: "10.0.0.3"}}}}
	servicesWatchedServices := []probe.S3Service{{Name: "s1", Endpoint: "10.0.0.1", GatewayReadEnpoints: []probe.S3Endpoint{{Name: "10.0.0.3"}}}}
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	if len(serviceToAdd) != 1 || len(serviceToRemove) != 1 {
		t.Errorf("getServicesToModify should have return s1 service in both serviceToRemove and serviceToAdd")
	}
}

func TestGetServicesToModifyReturnsEmptyWhenNoChange(t *testing.T) {
	servicesFromConsul := []probe.S3Service{{Name: "s1", Endpoint: "10.0.0.1", GatewayReadEnpoints: []probe.S3Endpoint{{Name: "10.0.0.2"}, {Name: "10.0.0.3"}}}}
	servicesWatchedServices := []probe.S3Service{{Name: "s1", Endpoint: "10.0.0.1", GatewayReadEnpoints: []probe.S3Endpoint{{Name: "10.0.0.2"}, {Name: "10.0.0.3"}}}}
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	if len(serviceToAdd) != 0 || len(serviceToRemove) != 0 {
		t.Errorf("getServicesToModify shouldn't have returned serviceToRemove and serviceToAdd")
	}
}

func TestFlushOldProbesSendStopCommand(t *testing.T) {
	controlChan := make(chan bool, 1)
	w := Watcher{
		watchedServices: map[string]watchedService{"test": {service: probe.S3Service{Name: "test"}, probeChan: controlChan}},
	}
	if len(controlChan) != 0 {
		t.Errorf("Chan not empty by default")
	}
	w.flushOldProbes(s3ServicesFromStrings([]string{"test"}))
	if len(controlChan) != 1 {
		t.Errorf("Stop command not received")
	}
	result := assert.So(w.watchedServices, should.NotContainKey, "test")
	if result.Failed() {
		t.Errorf("The assertion failed: %s", result)
	}
}
