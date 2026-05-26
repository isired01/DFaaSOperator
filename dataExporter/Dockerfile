# Stage 1: Build
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -o exporter main.go

# Stage 2: Final Image
FROM alpine:3.23
WORKDIR /root/
COPY --from=builder /app/exporter .
CMD ["./exporter"]