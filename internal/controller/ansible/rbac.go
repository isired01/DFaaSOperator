/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package ansible

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	dfaasv1 "dfaas-operator/api/v1"
)

// ensureRBAC creates / refreshes the per-env ServiceAccount + Role +
// RoleBinding that the Ansible Job pod uses to push the k6 kubeconfig Secret
// back into env.Namespace via `delegate_to: localhost`. Returns the SA name
// to be set on Job.Spec.Template.Spec.ServiceAccountName.
func (m *Manager) ensureRBAC(ctx context.Context, env *dfaasv1.Environment) (string, error) {
	saName := env.Name + "-ansible-sa"
	roleName := env.Name + "-ansible-role"
	bindingName := env.Name + "-ansible-rb"

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: env.Namespace},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, m.Client, sa, func() error {
		return ctrl.SetControllerReference(env, sa, m.Scheme)
	}); err != nil {
		return "", err
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: roleName, Namespace: env.Namespace},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, m.Client, role, func() error {
		role.Rules = []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"secrets"},
			Verbs:     []string{"get", "list", "create", "update", "patch"},
		}}
		return ctrl.SetControllerReference(env, role, m.Scheme)
	}); err != nil {
		return "", err
	}

	binding := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: bindingName, Namespace: env.Namespace},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, m.Client, binding, func() error {
		binding.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      saName,
			Namespace: env.Namespace,
		}}
		binding.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     roleName,
		}
		return ctrl.SetControllerReference(env, binding, m.Scheme)
	}); err != nil {
		return "", err
	}

	return saName, nil
}
