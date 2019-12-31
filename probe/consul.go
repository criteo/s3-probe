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
	destinations := extractDestinations(serviceEntries)

	s3endpoints := []s3endpoint{}
	health := consulClient.Health()

	for i := range destinations {

		endpointEntries, _, err := health.Service(destinations[i].service, "", false, &consul_api.QueryOptions{Datacenter: destinations[i].datacenter})
		if err != nil {
			log.Printf("Consul query failed for %s (dc: %s, service: %s): %s", destinations[i].raw, destinations[i].datacenter, destinations[i].service, err)
		}
		endpointName := getEndpointFromConsul(destinations[i].service, *cfg.EndpointSuffix, endpointEntries)
		minioClient, err := newMinioClientFromEndpoint(endpointName, *cfg.AccessKey, *cfg.SecretKey)
		if err != nil {
			log.Printf("Could not create minio client for %s (dc: %s, service: %s) : %s", destinations[i].raw, destinations[i].datacenter, destinations[i].service, err)
		}
		s3endpoints = append(s3endpoints, s3endpoint{name: endpointName, s3Client: minioClient})
		log.Printf("Added gateway destination: %s", endpointName)
	}
	return s3endpoints
}

type destination struct {
	datacenter string
	service    string
	raw        string
}

func extractDestinations(serviceEntries []*consul_api.ServiceEntry) (destinations []destination) {
	ok := false
	rawDestinations := ""
	for i := range serviceEntries {
		rawDestinations, ok = serviceEntries[i].Service.Meta["gateway_destinations"]
		if ok {
			break
		}
	}

	log.Printf("Processing gateway destinations: %s", rawDestinations)
	rawDestinationList := strings.Split(rawDestinations, ";")
	re := regexp.MustCompile("^(.*):(.*)$")

	for i := range rawDestinationList {
		match := re.FindStringSubmatch(rawDestinationList[i])
		if len(match) < 2 {
			continue
		}
		destinations = append(destinations, destination{raw: match[0], datacenter: match[1], service: match[2]})
	}
	return destinations
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
