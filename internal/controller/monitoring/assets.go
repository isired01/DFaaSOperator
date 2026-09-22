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

// liveDashboardUID is the dashboard's stable id: the UI deep-links a running
// LoadTest to /d/<uid> with the run's window and the Environment preselected,
// so changing it breaks those links.
const liveDashboardUID = "dfaas-live"

// The dashboard is provisioned from this file rather than created through the
// Grafana API, because Grafana runs with persistence disabled: an API-created
// dashboard lives in a SQLite file on an emptyDir and dies with the pod. A
// file-provisioned one is rebuilt identically on every start.
//
//go:embed dashboards/dfaas-live.json
var liveDashboardJSON []byte

// Upstream 4.45.0 with one template patched: its shared/security-configmap.yaml
// calls `fromToml`, which the Helm engine defines only from 3.16 on, while this
// operator pins the SDK at 3.14.4 (see the go.mod replace). An undefined function
// is a PARSE error, so the whole chart failed to render — "install release
// seaweedfs: parse error at (…/security-configmap.yaml:21)" — even though that
// block never renders under our values, because seaweedfs.securityConfigEnabled
// is false. Every published chart from 4.20 up carries the call, and the last one
// without it (3.59) predates the allInOne layout our values are written against,
// so downgrading was not an option. The patch replaces the call with `dict`, which
// is what `fromToml ""` yields on a fresh install anyway. Drop the patch and go
// back to the pristine archive once the Helm SDK is raised past 3.16.
//
//go:embed charts/seaweedfs-4.45.0-dfaas1.tgz
var seaweedfsChartTGZ []byte

//go:embed values/seaweedfs-values.yaml
var seaweedfsValuesYAML []byte
