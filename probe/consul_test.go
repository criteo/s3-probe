package probe

import (
	"log"
	"reflect"
	"sort"
	"testing"

	consul_api "github.com/hashicorp/consul/api"
)

func getTestServiceEntries() (entries []*consul_api.ServiceEntry) {
	dummyNode := consul_api.Node{
		Datacenter: "us-east-1",
	}

	dummyService := consul_api.AgentService{
		Port: 8080,
		Meta: make(map[string]string),
	}

	entry := &consul_api.ServiceEntry{
		Node:    &dummyNode,
		Service: &dummyService,
	}
	entries = append(entries, entry)
	return entries
}

func TestGenerateEndointFromConsulWithoutProxyData(t *testing.T) {
	entries := getTestServiceEntries()
	endpoint, err := getEndpointFromConsul("test", ".{dc}.prod", entries)
	if endpoint != "test.us-east-1.prod:8080" || err != nil {
		t.Errorf("Failed to generate URL from Consul data")
	}
}

func TestGenerateEndointFromConsulWithProxyData(t *testing.T) {
	entries := getTestServiceEntries()
	entries[0].Service.Meta["proxy_address"] = "foo.bar"
	endpoint, err := getEndpointFromConsul("test", ".{dc}.prod", entries)
	if endpoint != "foo.bar" || err != nil {
		t.Errorf("Failed to generate URL from proxy_address data")
	}
}

func TestExtractDestinations(t *testing.T) {
	dst1 := destination{datacenter: "us-east-2", service: "barfoo", raw: "us-east-2:barfoo"}
	dst2 := destination{datacenter: "us-west-1", service: "foobar", raw: "us-west-1:foobar"}
	dummyDestinations := []destination{dst1, dst2}
	entries := getTestServiceEntries()
	entries[0].Service.Meta["gateway_destinations"] = "us-west-1:foobar;us-east-2:barfoo"
	destinations := extractDestinations(entries)
	sort.SliceStable(destinations, func(i, j int) bool {
		return destinations[i].service < destinations[j].service
	})

	log.Println(dummyDestinations, destinations)
	if !reflect.DeepEqual(dummyDestinations, destinations) {
		t.Errorf("Failed to generate URL from proxy_address data")
	}
}
