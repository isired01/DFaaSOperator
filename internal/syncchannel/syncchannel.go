/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package syncchannel is the authless object channel the operator and the k6
// VMs share on the SeaweedFS filer: the GO signal that opens the Sync barrier,
// and the per-Generator end-of-test summaries.
//
// It exists because one SeaweedFS instance had three notions of where it is,
// chosen per call site: an in-cluster DNS const baked into the exporter Job, a
// DFAAS_FILER_URL-or-DNS function for the operator's own writes, and a
// DFAAS_SYNC_PUBLIC_URL-or-HOST_IP function for what the VMs poll. Every
// operation picked one by hand.
//
// The bases are resolved ONCE, at construction. That is the whole point: the
// three publish/delete functions were previously reachable in tests only by
// mutating process-global state with bare os.Setenv, which meant the specs
// could not run in parallel and a crashed spec leaked the variables into every
// later spec in the package.
package syncchannel

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/monitoring"
	"dfaas-operator/internal/k6dispatch"
)

// requestTimeout caps each filer HTTP call.
const requestTimeout = 10 * time.Second

// Channel is the object channel. Every method takes the LoadTest rather than a
// path, so no caller ever spells a filer path or picks a base.
type Channel interface {
	// PublishGo makes the GO object visible to every parked runner within one
	// poll interval. Idempotent: a re-PUT rewrites the object.
	PublishGo(ctx context.Context, lt *dfaasv1.LoadTest) error
	// DeleteGo and DeleteSummaries are best-effort hygiene: an already-absent
	// object is success, so a 404 is not an error.
	DeleteGo(ctx context.Context, lt *dfaasv1.LoadTest) error
	DeleteSummaries(ctx context.Context, lt *dfaasv1.LoadTest) error
	// GoURL is injected into the runner as DFAAS_SYNC_URL. Empty means no
	// public base is resolvable -- the caller MUST fail the LoadTest loudly
	// rather than dispatch a barrier nobody can open.
	GoURL(lt *dfaasv1.LoadTest) string
	// SummaryURL is injected as DFAAS_SUMMARY_URL. Empty means "no upload",
	// never an error: a missing summary only degrades data richness.
	SummaryURL(lt *dfaasv1.LoadTest, nodeID string) string
	// InClusterSummaryURL is what the exporter Job fetches. Always the
	// in-cluster base, never the public one.
	InClusterSummaryURL(lt *dfaasv1.LoadTest, nodeID string) string
}

// Filer is the production adapter.
type Filer struct {
	// operatorBase is what the operator itself dials.
	operatorBase string
	// publicBase is what the k6 VMs dial. Empty when unresolvable.
	publicBase string
	// inClusterBase is what the exporter Job dials -- always in-cluster DNS,
	// even when the operator itself is reaching the filer another way.
	inClusterBase string
	client        *http.Client
}

var _ Channel = (*Filer)(nil)

// NewFiler builds a Filer over two resolved bases. An empty operatorBase falls
// back to the in-cluster filer Service; an empty publicBase means the VM-facing
// URLs are unavailable, which GoURL and SummaryURL report as "".
func NewFiler(operatorBase, publicBase string) *Filer {
	inCluster := monitoring.FilerInClusterBase()
	op := strings.TrimRight(operatorBase, "/")
	if op == "" {
		op = inCluster
	}
	return &Filer{
		operatorBase:  op,
		publicBase:    strings.TrimRight(publicBase, "/"),
		inClusterBase: inCluster,
		client:        &http.Client{},
	}
}

// FromEnv resolves both bases from the process environment. Call it once, in
// cmd/main.go -- never per operation.
//
//   - DFAAS_FILER_URL overrides what the operator dials. In-cluster the Service
//     DNS works; under `make run` outside the cluster it does not resolve, so
//     the override points at the filer NodePort instead.
//   - DFAAS_SYNC_PUBLIC_URL overrides what the VMs dial (Helm-injectable, for
//     multi-subnet labs), else HOST_IP (downward API, status.hostIP on the
//     manager Deployment) plus the filer NodePort.
func FromEnv() *Filer {
	public := strings.TrimRight(os.Getenv("DFAAS_SYNC_PUBLIC_URL"), "/")
	if public == "" {
		if ip := os.Getenv("HOST_IP"); ip != "" {
			public = "http://" + net.JoinHostPort(ip, fmt.Sprint(monitoring.FilerNodePort))
		}
	}
	return NewFiler(os.Getenv("DFAAS_FILER_URL"), public)
}

// goPath is the filer path of the GO object. Outside /buckets, so it never
// shows up as an S3 bucket.
func goPath(lt *dfaasv1.LoadTest) string {
	return fmt.Sprintf("/dfaas-sync/%s/%s.go", lt.Namespace, lt.Name)
}

// SummaryDirPath is the per-LoadTest directory holding one summary object per
// Generator. Outside /buckets, like the GO object.
func SummaryDirPath(lt *dfaasv1.LoadTest) string {
	return fmt.Sprintf("/dfaas-k6-summary/%s/%s", lt.Namespace, lt.Name)
}

// SummaryPath is the filer path of one Generator's summary object. The nodeID
// is spelled with k6dispatch.Sanitize, the one definition of how a nodeID
// becomes part of a name -- drift here would 404 every fetch.
func SummaryPath(lt *dfaasv1.LoadTest, nodeID string) string {
	return fmt.Sprintf("%s/%s.json", SummaryDirPath(lt), k6dispatch.Sanitize(nodeID))
}

func (f *Filer) GoURL(lt *dfaasv1.LoadTest) string {
	if f.publicBase == "" {
		return ""
	}
	return f.publicBase + goPath(lt)
}

func (f *Filer) SummaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	if f.publicBase == "" {
		return ""
	}
	return f.publicBase + SummaryPath(lt, nodeID)
}

func (f *Filer) InClusterSummaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	return f.inClusterBase + SummaryPath(lt, nodeID)
}

func (f *Filer) PublishGo(ctx context.Context, lt *dfaasv1.LoadTest) error {
	resp, err := f.do(ctx, http.MethodPut, f.operatorBase+goPath(lt), strings.NewReader("go"))
	if err != nil {
		return fmt.Errorf("put GO signal: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("put GO signal: filer returned %s", resp.Status)
	}
	return nil
}

func (f *Filer) DeleteGo(ctx context.Context, lt *dfaasv1.LoadTest) error {
	return f.deleteBestEffort(ctx, f.operatorBase+goPath(lt), "delete GO signal")
}

func (f *Filer) DeleteSummaries(ctx context.Context, lt *dfaasv1.LoadTest) error {
	// One recursive DELETE instead of N per-node ones -- it also sweeps files
	// from Generators later removed from the spec.
	return f.deleteBestEffort(ctx,
		f.operatorBase+SummaryDirPath(lt)+"/?recursive=true", "delete k6 summaries")
}

// deleteBestEffort treats an already-absent object as success. The filer
// answers 204/202 on delete and 404 when the object is already gone.
func (f *Filer) deleteBestEffort(ctx context.Context, url, what string) error {
	resp, err := f.do(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("%s: filer returned %s", what, resp.Status)
	}
	return nil
}

func (f *Filer) do(ctx context.Context, method, url string, body *strings.Reader) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	var req *http.Request
	var err error
	if body == nil {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, body)
	}
	if err != nil {
		return nil, err
	}
	return f.client.Do(req)
}
