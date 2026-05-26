/*
Copyright 2026.

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
