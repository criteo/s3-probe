The probe listen to Consul and perform checks on every endpoints found.

This a probe for S3. There are three types of checks:
- Latency checks: the probe create, read and destroy and object and mesure the time taken by the operations.
- Durability checks: the probe when run for the first time creates N items into a bucket then count the number of items.
- Gateway checks: the probe use metadata from Consul to monitor a multi-cluster proxy gateway (see more in the dedicated part)

To reset the durability check, you need to remove the corresponding bucket, the probe will recreate it from scratch

# Gateway monitoring

A gateway in this context is a write only S3 compatible api that writes on multiple S3-like clusters. Writes are synchronous.
First the probe target the gateway Consul service, it then extract the `gateway_destinations` service metadata.
`gateway_destinations` value should be formatted as follow: `<dc>:<consul-service>;<dc>:<consul-service>, ...`
The probe will the write an object on the gateway and try to read it from all the destinations.

# Build

go 1.13 or above is required.

`GOOS=linux go build .`

# Docker build

```bash
docker build . s3-probe
```

#  Testing

Testing is mostly done with integration testing:
```
docker run -p 9000:9000 -d -e "MINIO_ACCESS_KEY=9PWM3PGAOU5TESTINGKEY" -e "MINIO_SECRET_KEY=p4KQAm5cLKfW2QoJG8SI5JOI3gYSECRETKEY" minio/minio server /data
go test -timeout 30s ./...
```