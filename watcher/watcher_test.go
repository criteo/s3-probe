package watcher

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/criteo/s3-probe/config"
	"github.com/criteo/s3-probe/probe"
)

func s3ServicesFromStrings(strings []string) (s3Services []probe.S3Service) {
	for i := range strings {
		s3Services = append(s3Services, probe.S3Service{Name: strings[i]})
	}
	return s3Services
}

func TestNewWrapperCreatesWrapper(t *testing.T) {
	tag := "randomTag"

	cfg := config.GetTestConfig()
	cfg.Tag = &tag
	w := NewWatcher(cfg)
	if w.consulTag != tag {
		t.Errorf("Constructor doesn't set tag properly")
	}
}

func TestGetWatchedServicesReturnTheCorrectEntries(t *testing.T) {
	testValues := []string{"test1", "test2", "test3"}
	testServices := s3ServicesFromStrings(testValues)
	pools := make(map[string](chan bool))
	for i := range testValues {
		pools[testValues[i]] = nil
	}
	w := Watcher{
		s3Pools: pools,
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

func TestFlushOldProbesSendStopCommand(t *testing.T) {
	controlChan := make(chan bool, 1)
	pools := make(map[string](chan bool))
	pools["test"] = controlChan
	w := Watcher{
		s3Pools: pools,
	}
	if len(controlChan) != 0 {
		t.Errorf("Chan not empty by default")
	}
	w.flushOldProbes(s3ServicesFromStrings([]string{"test"}))
	if len(controlChan) != 1 {
		t.Errorf("Stop command not received")
	}
}
