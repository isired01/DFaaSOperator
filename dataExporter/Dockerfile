# Stage 1: Build — native BUILDPLATFORM + Go cross-compile (no QEMU emulation).
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Build the package, not one file: the exporter is main.go + config.go +
# sink.go, and naming a single file silently drops the others.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -o exporter .

# Stage 2: Final Image
FROM alpine:3.23
WORKDIR /root/
COPY --from=builder /app/exporter .
CMD ["./exporter"]