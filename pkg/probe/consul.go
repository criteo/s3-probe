package probe

import (
	"log"
	"regexp"
	"strings"

	"github.com/criteo/s3-probe/pkg/config"

	consul_api "github.com/hashicorp/consul/api"
	"github.com/pkg/errors"
)

// ConsulClient is a wrapper around true consul client to ease mocking
type ConsulClient interface {
	GetAllMatchingRegisteredServices() (map[string]bool, error)
	GetServiceEndPoints(serviceName string, isGateway bool) (string, []S3Endpoint, error)
}

// concrete implementation
type consulClientImpl struct {
	cfg          *config.Config
	consulClient *consul_api.Client
}

// S3Service describe a S3 service and associated metadata
type S3Service struct {
	Name                string
	Endpoint            string
	Gateway             bool
	GatewayReadEnpoints []S3Endpoint
}

// Equals checks that to S3Service description are identical
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

// MakeConsulClient builds a new ConsulClient
func MakeConsulClient(cfg *config.Config) (ConsulClient, error) {
	defaultConfig := consul_api.DefaultConfig()
	defaultConfig.Address = *cfg.ConsulAddr

	client, err := consul_api.NewClient(defaultConfig)
	if err != nil {
		return nil, err
	}

	return &consulClientImpl{cfg: cfg, consulClient: client}, nil
}

// getAllMatchingRegisteredServices returns all registered services in consul that matched Tag or GatewayTag
func (cc *consulClientImpl) GetAllMatchingRegisteredServices() (map[string]bool, error) {
	catalog := cc.consulClient.Catalog()

	services, _, err := catalog.Services(nil)
	if err != nil {
		return map[string]bool{}, err
	}

	results := map[string]bool{}
	for serviceName := range services {
		for i := range services[serviceName] {
			isGateway := services[serviceName][i] == *cc.cfg.GatewayTag
			if isGateway || services[serviceName][i] == *cc.cfg.Tag {
				results[serviceName] = isGateway
				break
			}
		}
	}

	return results, nil
}

// getServiceEndPoint resolves the endpoint address of the given serviceName via consul
func (cc *consulClientImpl) GetServiceEndPoints(serviceName string, isGateway bool) (string, []S3Endpoint, error) {
	log.Printf("Fetching endpoints for service: %s", serviceName)
	health := cc.consulClient.Health()
	serviceEntries, _, err := health.Service(serviceName, "", false, nil)
	if err != nil {
		log.Printf("Fail to query health information for service %s from consul: %s\n", serviceName, err)
		return "", []S3Endpoint{}, err
	}

	endpoint, err := getEndpointFromConsul(serviceName, serviceEntries)
	if err != nil {
		log.Printf("Fail to resolve service endpoint from consul service entries for service %s: %s\n", serviceName, err)
		return "", []S3Endpoint{}, err
	}

	if isGateway {
		readEndpoints, err := extractGatewayEndoints(serviceEntries, cc.cfg, cc.consulClient)
		if err != nil {
			log.Printf("Resolving gateway endpoints failed for %s: %s", serviceName, err)
			return "", []S3Endpoint{}, err
		}
		return endpoint, readEndpoints, err
	}

	return endpoint, []S3Endpoint{}, nil
}

// NewProbeFromConsul Create a new probe using consul to generate endpoint configuration
func NewProbeFromConsul(service S3Service, cfg *config.Config, controlChan chan bool) (Probe, error) {
	return NewProbe(service, service.Endpoint, service.GatewayReadEnpoints, cfg, controlChan)
}

func getEndpointFromConsul(name string, serviceEntries []*consul_api.ServiceEntry) (string, error) {
	endpoint := ""
	if proxy, ok := getProxyEndpoint(serviceEntries); ok {
		endpoint = proxy
	} else {
		if externalClusterFqdn, ok := getExternalClusterFqdn(serviceEntries); ok {
			endpoint = externalClusterFqdn
		} else {
			return "", errors.Errorf("Endpoint name not found for %s", name)
		}
	}

	return endpoint, nil
}

func extractGatewayEndoints(serviceEntries []*consul_api.ServiceEntry, cfg *config.Config, consulClient *consul_api.Client) ([]S3Endpoint, error) {
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
		endpointName, err := getEndpointFromConsul(destination.service, endpointEntries)
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

func getExternalClusterFqdn(serviceEntries []*consul_api.ServiceEntry) (string, bool) {
	ok := false
	for i := range serviceEntries {
		value, ok := serviceEntries[i].Service.Meta["external_cluster_fqdn"]
		if ok {
			return value, ok
		}
	}
	return "", ok
}

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
