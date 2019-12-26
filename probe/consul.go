package probe

import (
	"fmt"

	"github.com/criteo/s3-probe/config"
	consul_api "github.com/hashicorp/consul/api"
)

// S3Service describe a S3 service and associated metadata
type S3Service struct {
	Name    string
	Gateway bool
}

// NewProbeFromConsul Create a new probe using consul to generate endpoint configuration
func NewProbeFromConsul(service S3Service, cfg config.Config, consulClient consul_api.Client, controlChan chan bool) (Probe, error) {
	health := consulClient.Health()
	serviceEntries, _, _ := health.Service(service.Name, "", false, nil)
	endpoint := ""
	if proxy, ok := getProxyEndpoint(serviceEntries); ok {
		endpoint = proxy
	} else {
		port := getServicePort(serviceEntries)
		endpoint = fmt.Sprintf("%s%s:%d", service.Name, *cfg.EndpointSuffix, port)
	}

	return NewProbe(service, endpoint, cfg, controlChan)
}

// getServicePort return the first port found in the service or 80
func getServicePort(serviceEntries []*consul_api.ServiceEntry) int {
	port := 80
	for i := range serviceEntries {
		port = serviceEntries[i].Service.Port
		break
	}
	return port
}

// getServicePort return the first port found in the service or 80
func getProxyEndpoint(serviceEntries []*consul_api.ServiceEntry) (string, bool) {
	ok := false
	proxy := ""
	for i := range serviceEntries {
		value, ok := serviceEntries[i].Service.Meta["proxy_address"]
		if ok {
			return value, ok
		}
	}
	return proxy, ok
}
