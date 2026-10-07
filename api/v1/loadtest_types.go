/*
Copyright 2026 Isaia Del Rosso.

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

// The two predicates the LoadTest reconciler branches on.

// Terminal reports whether the LoadTest has finished for good. A terminal
// LoadTest is immutable: spec.suspended and spec.stop are no longer read.
func (p LoadTestPhase) Terminal() bool {
	switch p {
	case LoadTestCompleted, LoadTestFailed, LoadTestAborted:
		return true
	default:
		return false
	}
}

// PreExecution reports whether the LoadTest has not started running yet --
// not yet admitted ("") or held at Pending. The create-time gates run only at
// "" (admission writes Pending before anything remote happens); the draft
// hold, the scheduled branch and the Environment wait apply to the whole
// window. It is the scope the API server would need a CEL transition rule to
// express.
func (p LoadTestPhase) PreExecution() bool {
	return p == "" || p == LoadTestPending
}

// Condition Types stamped on LoadTest.status.conditions.
const (
	LTCondReady             = "Ready"
	LTCondEnvironmentLinked = "EnvironmentLinked"
	LTCondScheduled         = "Scheduled"
	LTCondQueued            = "Queued"
	LTCondK6Dispatched      = "K6Dispatched"
	LTCondK6Healthy         = "K6Healthy"
	LTCondMetricsExported   = "MetricsExported"
	// LTCondSyncReady tracks the synchronized-start barrier (spec.syncStart):
	// False/AwaitingRunners while waiting for every TestRun to reach stage
	// "started", True/GoPublished once the GO signal is published.
	LTCondSyncReady = "SyncReady"
)

// Condition Reasons stamped on LoadTest.status.conditions.
const (
	LTReasonScheduledArmed         = "ScheduledArmed"
	LTReasonScheduledFired         = "ScheduledFired"
	LTReasonScheduledDelayedEnvNot = "ScheduledDelayedEnvNotReady"
	LTReasonInFlight               = "InFlight"
	LTReasonAllDispatched          = "AllDispatched"
	// LTReasonDispatchedUnreachable keeps K6Dispatched True (the TestRuns are
	// applied) and warns that at least one generator could not reach the
	// VM-facing filer address: its GO signal and summary upload will fail.
	LTReasonDispatchedUnreachable = "DispatchedUnreachable"
	LTReasonDispatchFailed        = "DispatchFailed"
	LTReasonPending               = "Pending"
	LTReasonAllFinished           = "AllFinished"
	LTReasonPartialFailure        = "PartialFailure"
	LTReasonAllFailed             = "AllFailed"
	LTReasonRunning               = "Running"
	LTReasonExportCooldown        = "ExportCooldown"
	LTReasonExporterRunning       = "ExporterRunning"
	LTReasonExportSucceeded       = "ExportSucceeded"
	LTReasonJobFailed             = "JobFailed"
	LTReasonExportSkipped         = "Skipped"
	// LTReasonRunnersReclaimed restamps K6Healthy when a run end confirmed
	// every remote TestRun absent. Without it the condition keeps the last
	// observed running count ("2 running") on a test whose runners are gone.
	LTReasonRunnersReclaimed = "RunnersReclaimed"
	// LTReasonRunnersUnreclaimed: a run end could not delete a TestRun this
	// run applied, so its runner may still be sending load. Occupancy holds
	// the Environment on it and the operator keeps retrying the delete.
	LTReasonRunnersUnreclaimed = "RunnersUnreclaimed"
	LTReasonS3ConfigMissing    = "S3ConfigMissing"
	LTReasonUserAborted        = "UserAborted"
	LTReasonCompleted          = "Completed"
	LTReasonFailed             = "Failed"
	LTReasonAborted            = "Aborted"
	LTReasonScriptMirrorFailed = "ScriptMirrorFailed"
	LTReasonStaleCleanupFailed = "StaleCleanupFailed"
	LTReasonAwaitingRunners    = "AwaitingRunners"
	LTReasonGoPublished        = "GoPublished"
	LTReasonSyncTimeout        = "SyncTimeout"
	LTReasonApplyFailed        = "ApplyFailed"
	// LTReasonFetchFailed marks an observe round in which some generator could
	// not report its TestRun's stage (K6Healthy=Unknown, "attempt n/15"), as
	// opposed to ApplyFailed which marks a failed write. Reusing ApplyFailed
	// for both made a dead k6 node read like a rejected manifest.
	LTReasonFetchFailed = "FetchFailed"
	LTReasonEnvNotFound = "EnvNotFound"
	LTReasonEnvFound    = "EnvFound"
	LTReasonEnvFailed   = "EnvFailed"
	// Queue-gate reasons (FIFO serialization on a shared Environment).
	LTReasonEnvBusy      = "EnvBusy"
	LTReasonQueuedBehind = "QueuedBehind"
	LTReasonDispatching  = "Dispatching"
)

// PerNodeLoad is the load of one k6-load-generator. The reconciler creates one
// remote TestRun per entry on that generator's k3s cluster.
type PerNodeLoad struct {
	// NodeID must match a k6-load-generator node in the target Environment.
	// Same DNS-1123 constraint as EnvironmentNode.NodeID: it is embedded in
	// remote TestRun and ConfigMap names.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`
	NodeID string `json:"nodeID"`

	// VUs is the virtual-user count declared for this generator. The operator
	// copies it into no TestRun field: k6 takes its virtual users from the
	// script's own options.
	// +kubebuilder:validation:Minimum=1
	VUs int `json:"vus"`

	// Duration is the run length declared for this generator, as a Go duration
	// string ("30s", "5m", "1h30m"). k6 never reads it: the script's options
	// decide how long it runs. The UI draws the generator's progress bar
	// against it.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$`
	Duration string `json:"duration"`

	// ScriptConfigMap references a ConfigMap in the same namespace whose key
	// "script.js" holds the k6 script for this generator. The UI writes one
	// ConfigMap per perNodeLoad entry, named <loadtest>-<nodeID>-script.
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

// MetricExportEntry describes one metric the exporter queries during the
// LoadTest's Exporting phase. All four fields reach the metrics CSV, as its
// query_type, query_name, query_expr and query_comment columns.
//
// Naming rule: when Type=raw and MetricName is empty, the operator falls
// back to MetricName = Query (the bare metric name doubles as alias).
// When Type=custom-promql, MetricName MUST be set (no sensible default
// from an arbitrary expression). Enforced via the CEL XValidation below.
//
// +kubebuilder:validation:XValidation:rule="self.type != 'custom-promql' || (has(self.metricName) && size(self.metricName) > 0)",message="metricName is required when type is custom-promql"
type MetricExportEntry struct {
	// Type is documentary: it classifies the query for the UI and fills the
	// CSV query_type column. The exporter runs Query verbatim either way.
	// +kubebuilder:validation:Required
	Type MetricsExportType `json:"type"`

	// MetricName names the query in the CSV query_name column. Required when
	// Type=custom-promql. Optional when Type=raw:
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

	// Comment is free text, copied into the CSV query_comment column.
	// +optional
	Comment string `json:"comment,omitempty"`
}

// MetricsExportSpec drives the exporter Job that runs after k6 finishes.
type MetricsExportSpec struct {
	// Metrics is the structured list of entries to export. Passed to the
	// exporter Job as the JSON-encoded env var METRICS_JSON.
	// +kubebuilder:validation:MinItems=1
	Metrics []MetricExportEntry `json:"metrics"`

	// Step is the query_range resolution, as a Go duration string (e.g. "15s",
	// "1m").
	// +kubebuilder:default="15s"
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))+$`
	Step string `json:"step,omitempty"`
}

// LoadTestSpec is the desired state of one load test against an Environment.
type LoadTestSpec struct {
	// Name of an Environment in the same namespace.
	// +kubebuilder:validation:Required
	TargetEnvironment string `json:"targetEnvironment"`

	// One entry per k6-load-generator node the test should hit. Keyed by
	// nodeID: the API server rejects duplicates at admission (listType=map),
	// which would otherwise produce two entries competing over the same remote
	// TestRun name.
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=nodeID
	PerNodeLoad []PerNodeLoad `json:"perNodeLoad"`

	// +kubebuilder:validation:Required
	MetricsExport MetricsExportSpec `json:"metricsExport"`

	// Suspended marks the LoadTest as a "Save as Draft": when true, the
	// reconciler keeps phase=Pending and does NOT dispatch any k6 TestRun.
	// PATCHing to false activates the test (phase advances to Running).
	// Once execution begins (phase=Running or beyond), changes to this field
	// are ignored — the run-once guard makes the state machine immutable
	// post-start. No Condition mirrors this field: consumers must read
	// spec.suspended itself to tell a draft apart from a test waiting on its
	// Environment.
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

	// SyncStart, when true, holds every generator at a barrier: each runner's
	// script polls a GO-signal URL in setup() (injected as DFAAS_SYNC_URL), and
	// the operator publishes the signal on the SeaweedFS filer once every
	// TestRun reports k6-operator's stage "started". That stage can be reported
	// before a runner's setup() reaches the barrier, so runners can still start
	// seconds apart. Requires the generators to reach the management node on
	// the filer NodePort (30901). If a runner errors, or not every runner has
	// started within 5 minutes, the test fails.
	// +kubebuilder:default=false
	// +optional
	SyncStart bool `json:"syncStart,omitempty"`
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
