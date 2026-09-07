package ansible

import _ "embed"

//go:embed templates/setup-nodes.yml
var ansiblePlaybook string

//go:embed templates/setup-k6-nodes.yml
var k6Playbook string

//go:embed templates/requirements.yml
var galaxyRequirements string

//go:embed templates/haproxy-values.yaml
var haproxyValues string

//go:embed templates/openfaas-values.yaml
var openfaasValues string

//go:embed templates/prometheus-values.yaml
var prometheusValues string

// playbooks maps a roles.Spec.Playbook filename to its embedded content, so
// the ConfigMap that ships a role's playbook is built from the role table
// instead of a switch that had to agree with two others.
var playbooks = map[string]string{
	"setup-nodes.yml":    ansiblePlaybook,
	"setup-k6-nodes.yml": k6Playbook,
}
