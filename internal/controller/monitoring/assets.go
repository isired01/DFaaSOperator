package monitoring

import _ "embed"

//go:embed charts/prometheus-29.6.0.tgz
var prometheusChartTGZ []byte

//go:embed charts/grafana-10.5.15.tgz
var grafanaChartTGZ []byte

//go:embed values/prometheus-values.yaml
var prometheusValuesYAML []byte

//go:embed values/grafana-values.yaml
var grafanaValuesYAML []byte

// minioYAML is the raw multi-doc manifest for the in-cluster MinIO instance
// that serves as the default S3 sink (Secret + PVC + Deployment + Service in
// the "monitoring" namespace). Unlike Prometheus/Grafana it is not a Helm
// chart; Deploy decodes and server-side-applies each object directly.
//
//go:embed minio.yaml
var minioYAML []byte
