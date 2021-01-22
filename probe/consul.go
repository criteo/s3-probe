package probe

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/criteo/s3-probe/config"
	consul_api "github.com/hashicorp/consul/api"
)

// S3Service describe a S3 service and associated metadata
type S3Service struct {
	Name                string
	Endpoint            string
	Gateway             bool
	GatewayReadEnpoints []S3Endpoint
}

func (s *S3Service) Equals(other *S3Service) bool {
	if s.Name != other.Name ||
		s.Endpoint != other.Endpoint ||
		s.Gateway != other.Gateway ||
		len(s.GatewayReadEnpoints) != len(other.GatewayReadEnpoints) {
		return false
	}

	for i, gatewayReadEndPoint := range s.GatewayReadEnpoints {
		if gatewayReadEndPoint.Name != other.GatewayReadEnpoints[i].Name {
			return false
		}
	}

	return true
}

// GetS3Services Discover S3 services from consul (S3 service must match given consulTag or consulGatewayTag)
func GetS3Services(cfg config.Config, consulClient consul_api.Client, consulTag string, consulGatewayTag string) []S3Service {
	catalog := consulClient.Catalog()
	health := consulClient.Health()
	services, _, _ := catalog.Services(nil)

	var s3Services []S3Service
	var service S3Service
	for serviceName := range services {
		for i := range services[serviceName] {
			isGateway := services[serviceName][i] == consulGatewayTag

			if isGateway || services[serviceName][i] == consulTag {
				serviceEntries, _, err := health.Service(service.Name, "", false, nil)
				if err != nil {
					log.Printf("Consul query failed for %s: %s", service.Name, err)
					break
				}

				endpoint, err := getEndpointFromConsul(service.Name, *cfg.EndpointSuffix, serviceEntries)
				if err != nil {
					log.Printf("Resolving service endpoint failed for %s: %s", service.Name, err)
					break
				}

				var readEndpoints []S3Endpoint
				if isGateway {
					readEndpoints, err = extractGatewayEndoints(serviceEntries, cfg, consulClient)
					if err != nil {
						log.Printf("Resolving gateway endpoints failed for %s: %s", service.Name, err)
						break
					}
				}

				service = S3Service{Name: serviceName, Endpoint: endpoint, Gateway: isGateway, GatewayReadEnpoints: readEndpoints}
				s3Services = append(s3Services, service)
				break
			}
		}
	}
	return s3Services
}

// NewProbeFromConsul Create a new probe using consul to generate endpoint configuration
func NewProbeFromConsul(service S3Service, cfg config.Config, consulClient consul_api.Client, controlChan chan bool) (Probe, error) {
	return NewProbe(service, service.Endpoint, service.GatewayReadEnpoints, cfg, controlChan)
}

func getEndpointFromConsul(name string, endpointSuffix string, serviceEntries []*consul_api.ServiceEntry) (string, error) {
	endpoint := ""
	if proxy, ok := getProxyEndpoint(serviceEntries); ok {
		endpoint = proxy
	} else {
		port, err := getServicePort(serviceEntries)
		if err != nil {
			return "", err
		}
		dc, err := getDatacenter(serviceEntries)
		endpointSuffixWithDC := strings.ReplaceAll(endpointSuffix, "{dc}", dc)
		endpoint = fmt.Sprintf("%s%s:%d", name, endpointSuffixWithDC, port)
	}

	return endpoint, nil
}

func extractGatewayEndoints(serviceEntries []*consul_api.ServiceEntry, cfg config.Config, consulClient consul_api.Client) ([]S3Endpoint, error) {
	s3endpoints := []S3Endpoint{}

	destinations, err := extractDestinations(serviceEntries)
	if err != nil {
		return s3endpoints, err
	}

	health := consulClient.Health()

	for _, destination := range destinations {

		endpointEntries, _, err := health.Service(destination.service, "", false, &consul_api.QueryOptions{Datacenter: destination.datacenter})
		if err != nil {
			log.Printf("Consul query failed for %s (dc: %s, service: %s): %s", destination.raw, destination.datacenter, destination.service, err)
			return s3endpoints, err
		}
		endpointName, err := getEndpointFromConsul(destination.service, *cfg.EndpointSuffix, endpointEntries)
		if err != nil {
			return s3endpoints, err
		}
		minioClient, err := newMinioClientFromEndpoint(endpointName, *cfg.AccessKey, *cfg.SecretKey)
		if err != nil {
			log.Printf("Could not create minio client for %s (dc: %s, service: %s) : %s", destination.raw, destination.datacenter, destination.service, err)
			return []S3Endpoint{}, err
		}
		s3endpoints = append(s3endpoints, S3Endpoint{Name: endpointName, s3Client: minioClient})
		log.Printf("Added gateway destination: %s", endpointName)
	}
	return s3endpoints, nil
}

type destination struct {
	datacenter string
	service    string
	raw        string
}

func extractDestinations(serviceEntries []*consul_api.ServiceEntry) (destinations []destination, err error) {
	rawDestinations := ""
	for i := range serviceEntries {
		if dst, ok := serviceEntries[i].Service.Meta["gateway_destinations"]; ok {
			rawDestinations = dst
		}
	}

	log.Printf("Processing gateway destinations: %s", rawDestinations)
	rawDestinationList := strings.Split(rawDestinations, ";")
	re := regexp.MustCompile("^(.*):(.*)$")

	for i := range rawDestinationList {
		match := re.FindStringSubmatch(rawDestinationList[i])
		if len(match) < 2 {
			log.Println("Failed to match: ", rawDestinationList[i])
			return destinations, errors.New("Error, failed to extract destinations")
		}
		destinations = append(destinations, destination{raw: match[0], datacenter: match[1], service: match[2]})
	}
	return destinations, nil
}

// getServicePort return the first port found in the service or 80
func getServicePort(serviceEntries []*consul_api.ServiceEntry) (int, error) {
	if len(serviceEntries) == 0 {
		return 80, errors.New("Consul service is empty")
	}
	return serviceEntries[0].Service.Port, nil
}

func getDatacenter(serviceEntries []*consul_api.ServiceEntry) (string, error) {
	if len(serviceEntries) == 0 {
		return "", errors.New("Consul service is empty")
	}
	return serviceEntries[0].Node.Datacenter, nil
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
