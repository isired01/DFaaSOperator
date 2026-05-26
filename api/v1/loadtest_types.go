/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// LoadTestPhase tracks the lifecycle of a single load test.
// +kubebuilder:validation:Enum=Pending;Running;Exporting;Completed;Failed;Aborted
type LoadTestPhase string

const (
	LoadTestPending   LoadTestPhase = "Pending"
	LoadTestRunning   LoadTestPhase = "Running"
	LoadTestExporting LoadTestPhase = "Exporting"
	LoadTestCompleted LoadTestPhase = "Completed"
	LoadTestFailed    LoadTestPhase = "Failed"
	LoadTestAborted   LoadTestPhase = "Aborted"
)

// Condition Types stamped on LoadTest.status.conditions (P15).
const (
	LTCondReady             = "Ready"
	LTCondSuspended         = "Suspended"
	LTCondEnvironmentLinked = "EnvironmentLinked"
	LTCondScheduled         = "Scheduled"
	LTCondK6Dispatched      = "K6Dispatched"
	LTCondK6Healthy         = "K6Healthy"
	LTCondMetricsExported   = "MetricsExported"
	LTCondSpecLocked        = "SpecLocked"
)

// Condition Reasons stamped on LoadTest.status.conditions (P15).
const (
	LTReasonDraftSaved             = "DraftSaved"
	LTReasonActivated              = "Activated"
	LTReasonScheduledArmed         = "ScheduledArmed"
	LTReasonScheduledFired         = "ScheduledFired"
	LTReasonScheduledDelayedEnvNot = "ScheduledDelayedEnvNotReady"
	LTReasonNotScheduled           = "NotScheduled"
	LTReasonInFlight               = "InFlight"
	LTReasonAllDispatched          = "AllDispatched"
	LTReasonDispatchFailed         = "DispatchFailed"
	LTReasonPending                = "Pending"
	LTReasonAllFinished            = "AllFinished"
	LTReasonPartialFailure         = "PartialFailure"
	LTReasonAllFailed              = "AllFailed"
	LTReasonRunning                = "Running"
	LTReasonK6Running              = "K6Running"
	LTReasonExporterRunning        = "ExporterRunning"
	LTReasonExportSucceeded        = "ExportSucceeded"
	LTReasonJobFailed              = "JobFailed"
	LTReasonExportSkipped          = "Skipped"
	LTReasonS3ConfigMissing        = "S3ConfigMissing"
	LTReasonUserAborted            = "UserAborted"
	LTReasonCompleted              = "Completed"
	LTReasonFailed                 = "Failed"
	LTReasonAborted                = "Aborted"
	LTReasonScriptMirrorFailed     = "ScriptMirrorFailed"
	LTReasonStaleCleanupFailed     = "StaleCleanupFailed"
	LTReasonApplyFailed            = "ApplyFailed"
	LTReasonPostStart              = "PostStart"
	LTReasonEnvNotFound            = "EnvNotFound"
	LTReasonEnvFound               = "EnvFound"
	LTReasonEnvFailed              = "EnvFailed"
	LTReasonEnvDegraded            = "EnvDegraded"
)

// PerNodeLoad is the FE-supplied per-k6-machine load. The reconciler creates
// one remote TestRun per entry against the matching k6-load-generator node's
// k3s cluster.
type PerNodeLoad struct {
	// NodeID must match a k6-load-generator node in the target Environment.
	// +kubebuilder:validation:Required
	NodeID string `json:"nodeID"`

	// k6 virtual-users for this machine.
	// +kubebuilder:validation:Minimum=1
	VUs int `json:"vus"`

	// k6 test duration (Go duration string, e.g. "30s", "5m").
	// +kubebuilder:validation:Required
	Duration string `json:"duration"`

	// ScriptConfigMap references a ConfigMap in the same namespace exposing
	// key "script.js" with the k6 script for this machine. The FE writes one
	// ConfigMap per LoadTest.
	// +kubebuilder:validation:Required
	ScriptConfigMap corev1.LocalObjectReference `json:"scriptConfigMap"`
}

// MetricsExportType discriminates how the entry's query is interpreted
// (documentary; the backend executes the query string as-is in both cases).
// +kubebuilder:validation:Enum=raw;custom-promql
type MetricsExportType string

const (
	// MetricTypeRaw: query is a bare Prometheus metric name (no functions).
	MetricTypeRaw MetricsExportType = "raw"
	// MetricTypeCustomPromQL: query is an arbitrary PromQL expression.
	MetricTypeCustomPromQL MetricsExportType = "custom-promql"
)

// MetricExportEntry describes one metric the exporter should query during
// the LoadTest's Exporting phase. All four fields participate in the CSV
// output (Type and Comment become metadata columns; MetricName is the alias
// the CSV uses as identifier; Query is the PromQL string actually executed).
//
// Naming rule: when Type=raw and MetricName is empty, the operator falls
// back to MetricName = Query (the bare metric name doubles as alias).
// When Type=custom-promql, MetricName MUST be set (no sensible default
// from an arbitrary expression). Enforced via the CEL XValidation below.
//
// +kubebuilder:validation:XValidation:rule="self.type != 'custom-promql' || (has(self.metricName) && size(self.metricName) > 0)",message="metricName is required when type is custom-promql"
type MetricExportEntry struct {
	// Type is documentary — it classifies the query for the UI and the
	// CSV "type" column. The backend executes Query verbatim regardless.
	// +kubebuilder:validation:Required
	Type MetricsExportType `json:"type"`

	// MetricName is the alias / identifier surfaced in the CSV "metric"
	// column. Required when Type=custom-promql. Optional when Type=raw:
	// if empty, the operator falls back to MetricName = Query (so the
	// bare metric name doubles as alias).
	// +optional
	MetricName string `json:"metricName,omitempty"`

	// Query is the PromQL string sent to Prometheus. For Type=raw this
	// is a bare metric name (e.g. node_memory_MemAvailable_bytes); for
	// Type=custom-promql it is a full expression (e.g. rate(...)).
	// Required for both types.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`

	// Comment is free-text human description, surfaced into the CSV
	// "comment" column for post-test analysis.
	// +optional
	Comment string `json:"comment,omitempty"`
}

// MetricsExportSpec drives the exporter Job that runs after k6 finishes.
type MetricsExportSpec struct {
	// Metrics is the structured list of entries to export. Replaces the
	// old plaintext Queries []string; the new shape carries type, alias
	// and free-text comment alongside the PromQL string. Passed to the
	// exporter Job as the JSON-encoded env var METRICS_JSON.
	// +kubebuilder:validation:MinItems=1
	Metrics []MetricExportEntry `json:"metrics"`

	// Step for QueryRange (Go duration string).
	// +kubebuilder:default="15s"
	Step string `json:"step,omitempty"`
}

// LoadTestSpec is the desired state of one load test against an Environment.
type LoadTestSpec struct {
	// Name of an Environment in the same namespace.
	// +kubebuilder:validation:Required
	TargetEnvironment string `json:"targetEnvironment"`

	// One entry per k6-load-generator node the test should hit.
	// +kubebuilder:validation:MinItems=1
	PerNodeLoad []PerNodeLoad `json:"perNodeLoad"`

	// +kubebuilder:validation:Required
	MetricsExport MetricsExportSpec `json:"metricsExport"`

	// Suspended marks the LoadTest as a "Save as Draft": when true, the
	// reconciler keeps phase=Pending, stamps Conditions[Suspended]=True,
	// and does NOT dispatch any k6 TestRun. PATCHing to false activates
	// the test (the Suspended Condition flips to False, phase advances to
	// Running). Once execution begins (phase=Running or beyond), changes
	// to this field are ignored — the run-once guard makes the state
	// machine immutable post-start.
	//
	// Pattern mirrors batch/v1.Job.spec.suspend.
	// +kubebuilder:default=false
	// +optional
	Suspended bool `json:"suspended,omitempty"`

	// StartAt schedules the LoadTest to start at this wall-clock instant.
	// Requires Suspended=true. The controller PATCHes Suspended=false at fire
	// time when the target Environment is Ready. RFC3339 UTC.
	// +optional
	// +kubebuilder:validation:Format=date-time
	StartAt *metav1.Time `json:"startAt,omitempty"`

	// Stop, when set to true, triggers a multi-cluster cascading abort:
	// the reconciler deletes every remote TestRun for this LoadTest across
	// all k6-load-generator nodes, then marks the central CR terminal as
	// Aborted (preserving it as a historical record — the CR is NOT
	// deleted). Only honored while phase is "" / Pending / Running;
	// PATCHes after Exporting / Completed / Failed / Aborted are ignored
	// (the state machine is immutable post-terminal).
	// +kubebuilder:default=false
	// +optional
	Stop bool `json:"stop,omitempty"`
}

// TestRunRef records one remote TestRun dispatched on a k6 machine's k3s.
type TestRunRef struct {
	NodeID    string `json:"nodeID"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	// +optional
	Phase string `json:"phase,omitempty"`
}

// LoadTestStatus is the observed state of a LoadTest.
type LoadTestStatus struct {
	// +optional
	Phase LoadTestPhase `json:"phase,omitempty"`

	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// +optional
	EndTime *metav1.Time `json:"endTime,omitempty"`

	// +optional
	TestRuns []TestRunRef `json:"testRuns,omitempty"`

	// ExporterJob is the in-cluster Job name created during the Exporting
	// phase.
	// +optional
	ExporterJob string `json:"exporterJob,omitempty"`

	// +patchStrategy=merge
	// +patchMergeKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`

	// +optional
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Environment",type="string",JSONPath=".spec.targetEnvironment"
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
// LoadTest is the schema for a single load-test run against an Environment.
type LoadTest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              LoadTestSpec   `json:"spec,omitempty"`
	Status            LoadTestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type LoadTestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LoadTest `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LoadTest{}, &LoadTestList{})
}
