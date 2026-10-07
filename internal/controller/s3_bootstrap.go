/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// S3ConfigNamespace is the cluster-scoped registry namespace where the
// gateway stores S3 server configurations as labeled Secrets. The operator
// reads from this namespace when an Environment references an S3 config and
// mirrors the matching Secret into the LoadTest namespace at exporter time.
const S3ConfigNamespace = "dfaas-s3"

// S3ConfigLabel marks Secrets in S3ConfigNamespace as S3 server configs.
// Used by gateway list queries and by the operator's mirror copy to keep
// traceability across namespaces.
const S3ConfigLabel = "dfaas.io/s3-config"

// DefaultS3ConfigName is the name of the S3-config Secret pointing at the
// in-cluster SeaweedFS instance (monitoring/seaweedfs-all-in-one). It is the
// implicit sink used when an Environment carries no explicit spec.s3ConfigRef —
// metrics CSV and per-VM k6 logs default to SeaweedFS instead of stdout. Created
// at operator startup by EnsureDefaultS3Config. This name is part of the shared
// contract with the UI gateway and must match on both sides.
const DefaultS3ConfigName = "seaweedfs-default"

// EnsureS3Namespace idempotently creates the dfaas-s3 namespace that holds
// the S3-config Secrets registry. Called once at operator startup (before
// the manager starts) so the gateway can write configs without a chicken-
// and-egg dependency on the first Environment reconcile.
func EnsureS3Namespace(ctx context.Context, c client.Client) error {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: S3ConfigNamespace}}
	if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s namespace: %w", S3ConfigNamespace, err)
	}
	return nil
}

// defaultS3Endpoint is the in-cluster S3 endpoint of the SeaweedFS Helm
// release: Service seaweedfs-all-in-one in the monitoring namespace, S3 gateway
// on 8333. Must track the release/values in internal/controller/monitoring.
const defaultS3Endpoint = "http://seaweedfs-all-in-one.monitoring.svc.cluster.local:8333"

// EnsureDefaultS3Config idempotently creates the dfaas-s3/seaweedfs-default
// Secret describing the in-cluster SeaweedFS sink
// (monitoring/seaweedfs-all-in-one). The keys match what the exporter consumes
// (endpoint/region/access_key_id/secret_access_key/force_path_style) and the
// credentials mirror the s3.credentials in
// monitoring/values/seaweedfs-values.yaml, from which the chart renders the
// monitoring/seaweedfs-s3-secret identity file.
// Create-if-not-exists semantics: an AlreadyExists is treated as success so an
// admin who hand-edits the Secret (e.g. to point at an external S3) is never
// clobbered on restart.
//
// Called once at operator startup, after EnsureS3Namespace, using the same
// bootstrap client. Requires the dfaas-s3 namespace to already exist.
func EnsureDefaultS3Config(ctx context.Context, c client.Client) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DefaultS3ConfigName,
			Namespace: S3ConfigNamespace,
			Labels:    map[string]string{S3ConfigLabel: "true"},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"endpoint":          defaultS3Endpoint,
			"region":            "us-east-1",
			"access_key_id":     "admin",
			"secret_access_key": "admin123",
			"force_path_style":  "true",
		},
	}
	err := c.Create(ctx, secret)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s secret: %w", S3ConfigNamespace, DefaultS3ConfigName, err)
	}
	return nil
}
