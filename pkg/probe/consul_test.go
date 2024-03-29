package probe

import (
	"log"
	"reflect"
	"sort"
	"testing"

	consul_api "github.com/hashicorp/consul/api"
)

func TestS3ServiceEquals(t *testing.T) {
	service := S3Service{Name: "my-service", Endpoint: "127.0.0.1", Gateway: true, GatewayReadEnpoints: []S3Endpoint{{Name: "127.0.0.2"}, {Name: "127.0.0.3"}}}

	otherService := S3Service{Name: "my-service", Endpoint: "127.0.0.5", Gateway: true, GatewayReadEnpoints: []S3Endpoint{{Name: "127.0.0.2"}, {Name: "127.0.0.3"}}}
	if service.Equals(&otherService) {
		t.Error("S3Service equality should have return false due to different endpoint")
	}

	otherService = S3Service{Name: "my-service", Endpoint: "127.0.0.1", Gateway: false, GatewayReadEnpoints: []S3Endpoint{}}
	if service.Equals(&otherService) {
		t.Error("S3Service equality should have return false due to different Gateway flage")
	}

	otherService = S3Service{Name: "my-service", Endpoint: "127.0.0.1", Gateway: true, GatewayReadEnpoints: []S3Endpoint{}}
	if service.Equals(&otherService) {
		t.Error("S3Service equality should have return false due to different list of gateway read endpoints")
	}

	otherService = S3Service{Name: "my-service", Endpoint: "127.0.0.1", Gateway: true, GatewayReadEnpoints: []S3Endpoint{{Name: "127.0.0.2"}}}
	if service.Equals(&otherService) {
		t.Error("S3Service equality should have return false due to different list of gateway read endpoints")
	}

	otherService = S3Service{Name: "my-service", Endpoint: "127.0.0.1", Gateway: true, GatewayReadEnpoints: []S3Endpoint{{Name: "127.0.0.3"}, {Name: "127.0.0.2"}}}
	if service.Equals(&otherService) {
		t.Error("S3Service equality should have return false due to different list of gateway read endpoints")
	}

	otherService = S3Service{Name: "my-service", Endpoint: "127.0.0.1", Gateway: true, GatewayReadEnpoints: []S3Endpoint{{Name: "127.0.0.2"}, {Name: "127.0.0.3"}}}
	if !service.Equals(&otherService) {
		t.Error("S3Service equality should have return true due to perfect deep equality between both services")
	}
}

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
	entries[0].Service.Meta["external_cluster_fqdn"] = "http://test.us-east-1.prod:8080"
	endpoint, err := getEndpointFromConsul("test", entries)
	if endpoint != "http://test.us-east-1.prod:8080" || err != nil {
		t.Errorf("Failed to generate URL from Consul data")
	}
}

func TestGenerateEndointFromConsulWithProxyData(t *testing.T) {
	entries := getTestServiceEntries()
	entries[0].Service.Meta["proxy_address"] = "foo.bar"
	endpoint, err := getEndpointFromConsul("test", entries)
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
	destinations, _ := extractDestinations(entries)
	sort.SliceStable(destinations, func(i, j int) bool {
		return destinations[i].service < destinations[j].service
	})

	log.Println(dummyDestinations, destinations)
	if !reflect.DeepEqual(dummyDestinations, destinations) {
		t.Errorf("Failed to generate URL from proxy_address data")
	}
}

func TestGenerateEndointFailIfConsulServiceEmpty(t *testing.T) {
	entries := []*consul_api.ServiceEntry{}
	_, err := getEndpointFromConsul("test", entries)
	if err == nil {
		t.Errorf("GenerateEndpoint should fail when given empty service")
	}
}

func TestExtractDestinationsFailIfWronglyFormatted(t *testing.T) {
	entries := getTestServiceEntries()
	entries[0].Service.Meta["gateway_destinations"] = "us-west-1foobar;us-east-2:barfoo"
	_, err := extractDestinations(entries)

	if err == nil {
		t.Errorf("Extract destination didn't fail on poorly formated destinations")
	}
}
