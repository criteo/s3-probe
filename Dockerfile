FROM alpine:3.7
COPY ./s3-probe /bin/s3-probe
EXPOSE 8080
ENTRYPOINT [ "/bin/s3-probe" ]


