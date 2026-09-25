/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package monitoring

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	sigsyaml "sigs.k8s.io/yaml"
)

// Retention lives in prometheus.yml (server.tsdb), which the config-reload
// sidecar hot-reloads, so changing it never restarts the shared Prometheus.
// server.retention stays as a flag because dropping it brings back the chart
// default (15d) as a flag, which changes the pod args and restarts the pod.
func TestPrometheusRetentionIsSizeBoundAndHotReloadable(t *testing.T) {
	var v struct {
		Server struct {
			Retention     string `json:"retention"`
			RetentionSize string `json:"retentionSize"`
			PV            struct {
				Size string `json:"size"`
			} `json:"persistentVolume"`
			TSDB struct {
				Retention struct {
					Time string `json:"time"`
					Size string `json:"size"`
				} `json:"retention"`
			} `json:"tsdb"`
		} `json:"server"`
	}
	if err := sigsyaml.Unmarshal(prometheusValuesYAML, &v); err != nil {
		t.Fatalf("parse prometheus values: %v", err)
	}
	s := v.Server
	if s.TSDB.Retention.Time == "" || s.TSDB.Retention.Time != s.Retention {
		t.Errorf("server.tsdb.retention.time = %q, want it equal to server.retention %q: "+
			"a retention block without time disables time-based retention", s.TSDB.Retention.Time, s.Retention)
	}
	if s.RetentionSize != "" {
		t.Errorf("server.retentionSize = %q: it is a pod flag and restarts Prometheus; "+
			"set server.tsdb.retention.size instead", s.RetentionSize)
	}
	if s.TSDB.Retention.Size == "" {
		t.Fatal("server.tsdb.retention.size is unset: a full volume stops ingestion instead of dropping old blocks")
	}
	// Prometheus size units are base-2 ("1KB is 1024B"): 16GB means 16Gi.
	size, err := resource.ParseQuantity(strings.TrimSuffix(s.TSDB.Retention.Size, "B") + "i")
	if err != nil {
		t.Fatalf("server.tsdb.retention.size %q: %v", s.TSDB.Retention.Size, err)
	}
	pv, err := resource.ParseQuantity(s.PV.Size)
	if err != nil {
		t.Fatalf("server.persistentVolume.size %q: %v", s.PV.Size, err)
	}
	// The size counts WAL and head chunks, and compaction needs a temporary
	// block: upstream advises at most 80-85% of the disk.
	if size.Value()*100 > pv.Value()*85 {
		t.Errorf("retention size %s is over 85%% of the %s volume", s.TSDB.Retention.Size, s.PV.Size)
	}
}
