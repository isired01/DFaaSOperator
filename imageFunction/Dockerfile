# syntax=docker/dockerfile:1
#
# Multi-arch OpenFaaS function image (of-watchdog http mode).
# release.yml publishes the image on every v* tag. Manual multi-arch build and
# push (no --load for multi-arch), from the repo root:
#   docker buildx build --platform linux/amd64,linux/arm64 \
#     -t ghcr.io/isired01/dfaas-imgproc:<tag> -f imageFunction/Dockerfile imageFunction --push

FROM --platform=${BUILDPLATFORM:-linux/amd64} golang:1.26-alpine3.22 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /handler .

# of-watchdog ships the fwatchdog binary; image is multi-arch (amd64/arm64).
FROM --platform=${TARGETPLATFORM:-linux/amd64} ghcr.io/openfaas/of-watchdog:0.11.4 AS watchdog

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S app && adduser -S -G app app
WORKDIR /home/app
COPY --from=watchdog /fwatchdog /usr/bin/fwatchdog
COPY --from=build /handler ./handler
RUN chmod +x /usr/bin/fwatchdog ./handler
USER app

# of-watchdog http mode: fwatchdog listens on :8080, starts ./handler once
# (fprocess) and proxies each request to upstream_url. handler listens on
# :8082 and stays resident — no fork per request.
ENV mode="http" \
    upstream_url="http://127.0.0.1:8082" \
    fprocess="./handler" \
    UPSTREAM_PORT="8082" \
    THUMB_SIZE="128" \
    prefix_logs="false"

EXPOSE 8080
CMD ["fwatchdog"]
