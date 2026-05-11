package ansible

import _ "embed"

//go:embed templates/setup-nodes.yml
var ansiblePlaybook string

//go:embed templates/haproxy-values.yaml
var haproxyValues string

//go:embed templates/openfaas-values.yaml
var openfaasValues string

//go:embed templates/prometheus-values.yaml
var prometheusValues string
