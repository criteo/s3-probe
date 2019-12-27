package probe

import (
	"fmt"
	"log"
	"regexp"
	"strings"

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

	endpoint := getEndpointFromConsul(service.Name, *cfg.EndpointSuffix, serviceEntries)

	readEndpoints := []s3endpoint{}
	if service.Gateway {
		readEndpoints = extractGatewayEndoints(serviceEntries, cfg, consulClient)
	}

	return NewProbe(service, endpoint, readEndpoints, cfg, controlChan)
}

func getEndpointFromConsul(name string, endpointSuffix string, serviceEntries []*consul_api.ServiceEntry) string {
	endpoint := ""
	if proxy, ok := getProxyEndpoint(serviceEntries); ok {
		endpoint = proxy
	} else {
		port := getServicePort(serviceEntries)
		dc := getDatacenter(serviceEntries)
		endpointSuffixWithDC := strings.ReplaceAll(endpointSuffix, "{dc}", dc)
		endpoint = fmt.Sprintf("%s%s:%d", name, endpointSuffixWithDC, port)
	}

	return endpoint
}

func extractGatewayEndoints(serviceEntries []*consul_api.ServiceEntry, cfg config.Config, consulClient consul_api.Client) []s3endpoint {
	ok := false
	destinationsRaw := ""
	for i := range serviceEntries {
		destinationsRaw, ok = serviceEntries[i].Service.Meta["gateway_destinations"]
		if ok {
			break
		}
	}

	log.Printf("Processing gateway destinations: %s", destinationsRaw)

	destinations := strings.Split(destinationsRaw, ";")
	re := regexp.MustCompile("^(.*):(.*)$")

	s3endpoints := []s3endpoint{}
	health := consulClient.Health()

	for i := range destinations {
		match := re.FindStringSubmatch(destinations[i])
		if len(match) < 2 {
			continue
		}
		endpointEntries, _, err := health.Service(match[2], "", false, &consul_api.QueryOptions{Datacenter: match[1]})
		if err != nil {
			log.Printf("Consul query failed for %s (dc: %s, service: %s): %s", match[0], match[1], match[2], err)
		}
		endpointName := getEndpointFromConsul(match[2], *cfg.EndpointSuffix, endpointEntries)
		minioClient, err := newMinioClientFromEndpoint(endpointName, *cfg.AccessKey, *cfg.SecretKey)
		if err != nil {
			log.Printf("Could not create minio client for %s (dc: %s, service: %s) : %s", match[0], match[1], match[2], err)
		}
		s3endpoints = append(s3endpoints, s3endpoint{name: endpointName, s3Client: minioClient})
		log.Printf("Added gateway destination: %s", endpointName)
	}
	return s3endpoints
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

func getDatacenter(serviceEntries []*consul_api.ServiceEntry) string {
	dc := ""
	for i := range serviceEntries {
		dc = serviceEntries[i].Node.Datacenter
		break
	}
	return dc
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
