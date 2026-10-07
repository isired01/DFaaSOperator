{{/*
Common helpers.
*/}}

{{- define "dfaas.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dfaas.operator.image" -}}
{{ .Values.operator.image.repository }}:{{ default .Chart.AppVersion .Values.operator.image.tag }}
{{- end -}}

{{- define "dfaas.exporter.image" -}}
{{ .Values.operator.exporterImage.repository }}:{{ default .Chart.AppVersion .Values.operator.exporterImage.tag }}
{{- end -}}

{{- define "dfaas.ui.image" -}}
{{ .Values.ui.image.repository }}:{{ default .Chart.AppVersion .Values.ui.image.tag }}
{{- end -}}

{{- define "dfaas.operator.sa" -}}
{{- default (printf "%s-operator" (include "dfaas.fullname" .)) .Values.operator.serviceAccount.name -}}
{{- end -}}

{{- define "dfaas.ui.sa" -}}
{{- default (printf "%s-ui" (include "dfaas.fullname" .)) .Values.ui.serviceAccount.name -}}
{{- end -}}

{{- define "dfaas.operator.labels" -}}
app.kubernetes.io/name: dfaas-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "dfaas.operator.selectorLabels" -}}
app.kubernetes.io/name: dfaas-operator
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "dfaas.ui.labels" -}}
app.kubernetes.io/name: dfaas-ui
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: ui
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{- define "dfaas.ui.selectorLabels" -}}
app.kubernetes.io/name: dfaas-ui
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
