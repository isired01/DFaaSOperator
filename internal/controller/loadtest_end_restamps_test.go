/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dfaasv1 "dfaas-operator/api/v1"
)

// Deleting a test while its exporter Job runs aborts it from Exporting. The
// terminal record must not show an export still in progress.
func TestEndRestampsClosesAnExportCutShort(t *testing.T) {
	lt := &dfaasv1.LoadTest{Status: dfaasv1.LoadTestStatus{
		Phase: dfaasv1.LoadTestExporting,
		Conditions: []metav1.Condition{{Type: dfaasv1.LTCondMetricsExported,
			Status: metav1.ConditionUnknown, Reason: dfaasv1.LTReasonExporterRunning}},
	}}
	for _, c := range endRestamps(lt, dfaasv1.LoadTestAborted, dfaasv1.LTReasonUserAborted) {
		if c.Type == dfaasv1.LTCondMetricsExported {
			if c.Status != metav1.ConditionFalse || c.Reason != dfaasv1.LTReasonExportSkipped ||
				!strings.Contains(c.Message, "stopped") {
				t.Errorf("MetricsExported restamp = %+v", c)
			}
			return
		}
	}
	t.Error("no MetricsExported restamp for a test aborted from Exporting")
}
