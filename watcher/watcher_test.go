package watcher

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestNewWrapperCreatesWrapper(t *testing.T) {
	addr := "localhost:9999"
	tag := "randomTag"
	w := NewWatcher(addr, tag, "", "", "", 0, 0, "", "", 120)
	if w.consulTag != tag {
		t.Errorf("Constructor doesn't set tag properly")
	}
}

func TestGetWatchedServicesReturnTheCorrectEntries(t *testing.T) {
	testValues := []string{"test1", "test2", "test3"}
	pools := make(map[string](chan bool))
	for i := range testValues {
		pools[testValues[i]] = nil
	}
	w := Watcher{
		s3Pools: pools,
	}
	services := w.getWatchedServices()
	sort.Strings(services)
	fmt.Println(testValues, services)
	if !reflect.DeepEqual(testValues, services) {
		t.Errorf("Returned value doesn't match given map")
	}
}

func TestGetServicesToModifyHandleLastFromConsul(t *testing.T) {
	servicesFromConsul := []string{"service1", "service2", "service5"}
	servicesWatchedServices := []string{"service2", "service3", "service4"}
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	expectedToAdd := []string{"service1", "service5"}
	expectedToRemove := []string{"service3", "service4"}
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
	servicesFromConsul := []string{"service1", "service2", "service5"}
	servicesWatchedServices := []string{"service0", "service3", "service5", "service6"}
	w := Watcher{}
	serviceToAdd, serviceToRemove := w.getServicesToModify(servicesFromConsul, servicesWatchedServices)
	expectedToAdd := []string{"service1", "service2"}
	expectedToRemove := []string{"service0", "service3", "service6"}
	if !reflect.DeepEqual(expectedToAdd, serviceToAdd) {
		fmt.Println(expectedToAdd, serviceToAdd)
		t.Errorf("return values to add are not the one expected")
	}
	if !reflect.DeepEqual(expectedToRemove, serviceToRemove) {
		fmt.Println(expectedToRemove, serviceToRemove)
		t.Errorf("return values to remove are not the one expected")
	}
}
