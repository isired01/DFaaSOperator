/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package fake is the in-memory monitoring adapter for tests. It records every
// call and answers from scripted sequences, so the five outcomes of
// ensureMonitoring -- including "Check returned an error" as distinct from
// "Check said false" -- are reachable with no Helm and no cluster.
package fake

import (
	"context"
	"sync"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/monitoring"
)

var _ monitoring.Stack = (*Stack)(nil)

// CheckResult is one scripted answer from Check.
type CheckResult struct {
	Ready bool
	Err   error
}

// Stack implements monitoring.Stack.
type Stack struct {
	mu sync.Mutex

	// DeployErrs is consumed one entry per Deploy call. When it runs out, the
	// last entry repeats -- a stack that fails keeps failing until a test says
	// otherwise. Empty means Deploy always succeeds.
	DeployErrs []error
	// CheckResults is consumed the same way. Empty means ready, no error.
	CheckResults []CheckResult
	// TargetsErr is returned by ReconcileTargets, CleanupErr by CleanupTargets.
	TargetsErr error
	CleanupErr error

	deploys  int
	checks   int
	targets  []string // one "namespace/name" entry per ReconcileTargets call
	cleanups []string
}

func (s *Stack) Deploy(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := nth(s.DeployErrs, s.deploys)
	s.deploys++
	return err
}

func (s *Stack) Check(_ context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.checks
	s.checks++
	if len(s.CheckResults) == 0 {
		return true, nil
	}
	if i >= len(s.CheckResults) {
		i = len(s.CheckResults) - 1
	}
	r := s.CheckResults[i]
	return r.Ready, r.Err
}

func (s *Stack) ReconcileTargets(_ context.Context, env *dfaasv1.Environment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.targets = append(s.targets, env.Namespace+"/"+env.Name)
	return s.TargetsErr
}

func (s *Stack) CleanupTargets(_ context.Context, env *dfaasv1.Environment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleanups = append(s.cleanups, env.Namespace+"/"+env.Name)
	return s.CleanupErr
}

// Deploys, Checks, TargetRounds and CleanupRounds report what the reconciler
// actually called.
func (s *Stack) Deploys() int { s.mu.Lock(); defer s.mu.Unlock(); return s.deploys }
func (s *Stack) Checks() int  { s.mu.Lock(); defer s.mu.Unlock(); return s.checks }

func (s *Stack) TargetRounds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.targets...)
}

func (s *Stack) CleanupRounds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.cleanups...)
}

// nth returns the i-th entry, or the last one once i runs past the end.
func nth(errs []error, i int) error {
	if len(errs) == 0 {
		return nil
	}
	if i >= len(errs) {
		i = len(errs) - 1
	}
	return errs[i]
}
