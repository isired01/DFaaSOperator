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
// dfaas-worker node in env. Resolution per node:
//
//  1. operator-managed Secret <envName>-libp2p-keys carries an entry for
//     nodeID → use it (covers both auto-generated keys and the BYO escape
//     hatch where the user pre-applies the Secret to pin a peer identity).
//  2. otherwise generate a fresh ed25519 PKCS#8 key, persist it into the
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

	// Prune entries for nodes that spec still lists but that no longer hold the
	// worker role: a flipped node runs no agent, so its identity is dead weight
	// in a Secret that is handed to every provisioning run. Deliberately scoped
	// to nodeIDs *present in spec* — an entry for a nodeID absent from spec is
	// the BYO escape hatch documented above (pre-seed the Secret to pin a peer
	// identity before adding the node) and must survive.
	prune := map[string]struct{}{}
	for _, n := range env.Spec.Nodes {
		if n.Role == dfaasv1.RoleDfaasWorker {
			continue
		}
		if _, ok := secret.Data[n.NodeID]; ok {
			prune[n.NodeID] = struct{}{}
			delete(secret.Data, n.NodeID)
			dirty = true
		}
	}

	if !dirty {
		return out, nil
	}

	if !exists {
		err := m.Create(ctx, &secret)
		if err == nil {
			return out, nil
		}
		if !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create libp2p keys secret: %w", err)
		}
		// Lost the create race (concurrent reconcile, or a BYO Secret applied
		// in between). The keys just generated were never persisted, so
		// returning them would hand the playbook a peer identity nobody else
		// agrees on — fall through to the merge path, which keeps whatever the
		// winner stored and reports it back.
	}

	merged, err := m.mergeLibp2pKeys(ctx, secretKey, secret.Data, prune)
	if err != nil {
		return nil, err
	}
	// Re-read every requested node off the persisted Secret: a concurrent
	// writer may have won on some nodeIDs, and its keys are authoritative.
	for nodeID := range out {
		if key, ok := merged[nodeID]; ok {
			out[nodeID] = key
		}
	}
	return out, nil
}

// mergeLibp2pKeys merges generated entries into the persisted Secret without
// clobbering existing ones (first writer wins per nodeID), removes the nodeIDs
// in prune, and returns the Secret's post-merge contents. The prune runs inside
// the same retry loop as the merge: dropping the entries locally before calling
// this would not stick, because the loop re-reads the latest Secret and would
// merge them straight back in.
func (m *Manager) mergeLibp2pKeys(ctx context.Context, secretKey client.ObjectKey,
	generated map[string][]byte, prune map[string]struct{}) (map[string]string, error) {

	merged := map[string]string{}
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &corev1.Secret{}
		if err := m.Get(ctx, secretKey, latest); err != nil {
			return err
		}
		if latest.Data == nil {
			latest.Data = map[string][]byte{}
		}
		for nodeID, key := range generated {
			if _, ok := latest.Data[nodeID]; !ok {
				latest.Data[nodeID] = key
			}
		}
		// After the merge, so a concurrent writer cannot resurrect an identity
		// for a node that has left the worker role.
		for nodeID := range prune {
			delete(latest.Data, nodeID)
		}
		if err := m.Update(ctx, latest); err != nil {
			return err
		}
		merged = make(map[string]string, len(latest.Data))
		for nodeID, key := range latest.Data {
			merged[nodeID] = string(key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("update libp2p keys secret: %w", err)
	}
	return merged, nil
}

// generateLibp2pKey returns a base64-encoded PKCS#8 ed25519 private key in the
// exact form expected by derivePeerID and the Helm chart `privateKey` value
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
