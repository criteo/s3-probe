# Build

`GOOS=linux go build .`

# Docker build

```bash
docker build . -t mesos-registry.par.preprod.crto.in/criteo/s3-probe
```

#  Testing

Testing is mostly done with integration testing:
```
docker run -p 9000:9000 -e "MINIO_ACCESS_KEY=9PWM3PGAOU5TESTINGKEY" -e "MINIO_SECRET_KEY=p4KQAm5cLKfW2QoJG8SI5JOI3gYSECRETKEY" minio/minio server /data
go test -timeout 30s ./...
```