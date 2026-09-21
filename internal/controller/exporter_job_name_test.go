package controller

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/ansible"
)

// A Job's name is copied verbatim into the auto-generated `job-name`
// pod-template label, and label values cap at 63 bytes. An over-long exporter
// Job name is therefore rejected at CREATE with
//
//	spec.template.labels: Invalid value: "<name>": must be no more than 63 bytes
//
// which wedges the LoadTest in Exporting and retries forever. This was hit in
// practice by a 41-character UI-generated LoadTest name.
func TestExporterJobNameFitsLabelValue(t *testing.T) {
	cases := []struct{ name, ltName string }{
		{"short", "lt-a"},
		{"typical UI name", "lt-bari-20260901-151639-syncon-f530f8"},
		{"the name that broke it", "lt-bari-20260901-152930-saturation-e76fcb"},
		{"absurdly long", strings.Repeat("x", 240)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lt := &dfaasv1.LoadTest{
				ObjectMeta: metav1.ObjectMeta{
					Name:       tc.ltName,
					UID:        types.UID("639f0735-1111-2222-3333-444455556666"),
					Generation: 1,
				},
			}
			got := ExporterJobName(lt)
			if len(got) > ansible.MaxJobNameLen {
				t.Fatalf("job name %q is %d bytes, exceeds the %d-byte label-value cap",
					got, len(got), ansible.MaxJobNameLen)
			}
			if errs := validation.IsDNS1123Subdomain(got); len(errs) > 0 {
				t.Fatalf("job name %q is not a valid DNS-1123 subdomain: %v", got, errs)
			}
			// The uniqueness-bearing suffix must survive truncation, otherwise two
			// generations of the same LoadTest would collide on one Job.
			if !strings.HasSuffix(got, "-exporter-639f0735-g1-job") {
				t.Fatalf("job name %q lost its uid/generation suffix", got)
			}
		})
	}
}

// Truncation must not collapse two distinct long names onto one Job.
func TestExporterJobNameStaysUniquePerGeneration(t *testing.T) {
	base := "lt-bari-20260901-152930-saturation-e76fcb"
	mk := func(gen int64) string {
		return ExporterJobName(&dfaasv1.LoadTest{ObjectMeta: metav1.ObjectMeta{
			Name: base, UID: types.UID("639f0735-1111-2222-3333-444455556666"), Generation: gen,
		}})
	}
	if a, b := mk(1), mk(2); a == b {
		t.Fatalf("generations 1 and 2 produced the same Job name %q", a)
	}
}

func TestBoundedJobNameKeepsShortNamesVerbatim(t *testing.T) {
	if got, want := ansible.BoundedJobName("env", "-infra-vms-abcd1234-g1-job"), "env-infra-vms-abcd1234-g1-job"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// The exporter writes the LoadTest and Environment names into every CSV row,
// so a run keeps its identity when ten of them are concatenated for analysis.
// Both used to be set only inside the S3 block, which left them empty on the
// stdout path -- and left the object keys built from an empty LoadTest name.
func TestExporterJobNamesTheRunWithoutS3(t *testing.T) {
	sch := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	r := &LoadTestReconciler{Scheme: sch}
	lt := &dfaasv1.LoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default", UID: types.UID("1"), Generation: 1},
		Spec: dfaasv1.LoadTestSpec{
			MetricsExport: dfaasv1.MetricsExportSpec{
				Metrics: []dfaasv1.MetricExportEntry{{Type: dfaasv1.MetricTypeRaw, Query: "up"}},
			},
		},
	}
	env := &dfaasv1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default", UID: types.UID("2")}}

	// No S3 config Secret: the stdout path, where these used to go missing.
	job, err := r.createExporterJob(lt, env, time.Now().Add(-time.Minute), time.Now(), "", nil)
	if err != nil {
		t.Fatalf("createExporterJob: %v", err)
	}
	got := map[string]string{}
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		got[e.Name] = e.Value
	}
	if got["LOADTEST_NAME"] != "lt-sample" {
		t.Errorf("LOADTEST_NAME = %q, want the LoadTest name", got["LOADTEST_NAME"])
	}
	if got["ENV_NAME"] != "bari" {
		t.Errorf("ENV_NAME = %q, want the Environment name", got["ENV_NAME"])
	}
}

// The exporter image is a moving tag: a merge to main republishes
// ghcr.io/isired01/dfaas-exporter:latest. With the default IfNotPresent, a
// node that already holds a layer for that tag reuses it, so a LoadTest can
// export with a build from weeks ago and nothing says so.
func TestExporterJobAlwaysPullsItsImage(t *testing.T) {
	sch := runtime.NewScheme()
	if err := dfaasv1.AddToScheme(sch); err != nil {
		t.Fatal(err)
	}
	r := &LoadTestReconciler{Scheme: sch}
	lt := &dfaasv1.LoadTest{
		ObjectMeta: metav1.ObjectMeta{Name: "lt-sample", Namespace: "default", UID: types.UID("1"), Generation: 1},
		Spec: dfaasv1.LoadTestSpec{
			MetricsExport: dfaasv1.MetricsExportSpec{
				Metrics: []dfaasv1.MetricExportEntry{{Type: dfaasv1.MetricTypeRaw, Query: "up"}},
			},
		},
	}
	env := &dfaasv1.Environment{ObjectMeta: metav1.ObjectMeta{Name: "bari", Namespace: "default", UID: types.UID("2")}}

	job, err := r.createExporterJob(lt, env, time.Now().Add(-time.Minute), time.Now(), "", nil)
	if err != nil {
		t.Fatalf("createExporterJob: %v", err)
	}
	if got := job.Spec.Template.Spec.Containers[0].ImagePullPolicy; got != corev1.PullAlways {
		t.Errorf("ImagePullPolicy = %q, want %q", got, corev1.PullAlways)
	}
}
