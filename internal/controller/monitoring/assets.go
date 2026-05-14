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
