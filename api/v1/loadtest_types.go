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
// +kubebuilder:validation:Enum=Pending;Running;Exporting;Completed;Failed
type LoadTestPhase string

const (
	LoadTestPending   LoadTestPhase = "Pending"
	LoadTestRunning   LoadTestPhase = "Running"
	LoadTestExporting LoadTestPhase = "Exporting"
	LoadTestCompleted LoadTestPhase = "Completed"
	LoadTestFailed    LoadTestPhase = "Failed"
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

// MetricsExportSpec drives the exporter Job that runs after k6 finishes.
type MetricsExportSpec struct {
	// PromQL queries pipe-joined and passed to the exporter as $QUERIES.
	// +kubebuilder:validation:MinItems=1
	Queries []string `json:"queries"`

	// Step for QueryRange (Go duration string).
	// +kubebuilder:default="15s"
	Step string `json:"step,omitempty"`

	// GoogleDrive is the optional CSV destination. nil → stdout.
	// +optional
	GoogleDrive *GoogleDriveConfig `json:"googleDrive,omitempty"`
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
