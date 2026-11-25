FROM cgr.dev/chainguard/minio-client@sha256:11aa4baa385a6de421008f72d5019805475f2aa7d74d65d18b92c54986f36016 AS client
FROM node:22-alpine
COPY --from=client /usr/bin/mc /usr/bin/mc
COPY --from=client /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY init.sh /init.sh
ENV MC_CONFIG_DIR=/tmp/mc
USER node
ENTRYPOINT ["/bin/sh", "/init.sh"]
