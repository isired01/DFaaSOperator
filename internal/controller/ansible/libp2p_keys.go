/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dfaasv1 "dfaas-operator/api/v1"
)

// EnsureLibp2pKeys returns per-nodeID base64 PKCS#8 ed25519 keys for every
// dfaas-worker node in env. Precedence at each call:
//
//  1. spec.PrivateKey is non-empty → use it (never mirrored into the Secret;
//     clearing the spec must NOT silently restore an older operator-managed key).
//  2. operator-managed Secret <envName>-libp2p-keys carries an entry for nodeID
//     → use it.
//  3. otherwise generate a fresh ed25519 PKCS#8 key, persist it into the
//     Secret, return it.
//
// The Secret is namespaced to env.Namespace and OwnerRef'd to env, so a
// `kubectl delete environment` cascades it.
func (m *Manager) EnsureLibp2pKeys(ctx context.Context,
	env *dfaasv1.Environment) (map[string]string, error) {

	secretName := env.Name + "-libp2p-keys"
	secretKey := client.ObjectKey{Name: secretName, Namespace: env.Namespace}

	var secret corev1.Secret
	exists := true
	if err := m.Get(ctx, secretKey, &secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get libp2p keys secret: %w", err)
		}
		exists = false
		secret = corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: env.Namespace,
			},
			Type: corev1.SecretTypeOpaque,
			Data: map[string][]byte{},
		}
		if err := ctrl.SetControllerReference(env, &secret, m.Scheme); err != nil {
			return nil, fmt.Errorf("set controller ref on libp2p secret: %w", err)
		}
	}
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}

	out := map[string]string{}
	dirty := false
	for _, n := range env.Spec.Nodes {
		if n.Role != dfaasv1.RoleDfaasWorker {
			continue
		}

		if n.PrivateKey != "" {
			out[n.NodeID] = n.PrivateKey
			continue
		}

		if existing, ok := secret.Data[n.NodeID]; ok && len(existing) > 0 {
			out[n.NodeID] = string(existing)
			continue
		}

		key, err := generateLibp2pKey()
		if err != nil {
			return nil, fmt.Errorf("generate libp2p key for node %q: %w", n.NodeID, err)
		}
		secret.Data[n.NodeID] = []byte(key)
		out[n.NodeID] = key
		dirty = true
	}

	if !dirty {
		return out, nil
	}

	if !exists {
		if err := m.Create(ctx, &secret); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create libp2p keys secret: %w", err)
		}
		return out, nil
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Secret{}
		if err := m.Get(ctx, secretKey, latest); err != nil {
			return err
		}
		if latest.Data == nil {
			latest.Data = map[string][]byte{}
		}
		for nodeID, key := range secret.Data {
			if _, ok := latest.Data[nodeID]; !ok {
				latest.Data[nodeID] = key
			}
		}
		return m.Update(ctx, latest)
	})
	if err != nil {
		return nil, fmt.Errorf("update libp2p keys secret: %w", err)
	}
	return out, nil
}

// generateLibp2pKey returns a base64-encoded PKCS#8 ed25519 private key in the
// exact form expected by calcolaPeerID and the Helm chart `privateKey` value
// at templates/setup-nodes.yml.
func generateLibp2pKey() (string, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(der), nil
}
