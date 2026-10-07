/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package fake is the in-memory syncchannel adapter for tests. It records every
// operation in order, so "the GO signal was published exactly once, and only
// after every TestRun reported started" is an assertion on a call list rather
// than a counter comparison on undifferentiated HTTP hits.
package fake

import (
	"context"
	"strings"
	"sync"

	dfaasv1 "dfaas-operator/api/v1"
	"dfaas-operator/internal/controller/monitoring"
	"dfaas-operator/internal/k6dispatch"
	"dfaas-operator/internal/syncchannel"
)

// Channel implements syncchannel.Channel.
type Channel struct {
	mu sync.Mutex

	// PublishErr, DeleteGoErr and DeleteSummariesErr are returned by the
	// matching operation. A filer that refuses the GO PUT is the case the old
	// httptest handler could not express: it answered 204 to everything.
	PublishErr         error
	DeleteGoErr        error
	DeleteSummariesErr error

	// Public is the process-wide fallback base, what a generator with no
	// detected management address dials (DFAAS_SYNC_PUBLIC_URL, else HOST_IP,
	// in production). A generator with one gets the filer NodePort on it
	// whatever Public says. Empty Public and no address mean unresolvable,
	// which is what makes GoURL return "" and a syncStart dispatch fail loudly.
	Public string

	calls []string
}

var _ syncchannel.Channel = (*Channel)(nil)

func (c *Channel) record(op string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, op)
}

// Calls returns the operations performed, in order: "PublishGo", "DeleteGo",
// "DeleteSummaries".
func (c *Channel) Calls() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.calls...)
}

// Count returns how many times one operation was performed.
func (c *Channel) Count(op string) int {
	n := 0
	for _, got := range c.Calls() {
		if got == op {
			n++
		}
	}
	return n
}

func (c *Channel) PublishGo(_ context.Context, _ *dfaasv1.LoadTest) error {
	c.record("PublishGo")
	return c.PublishErr
}

func (c *Channel) DeleteGo(_ context.Context, _ *dfaasv1.LoadTest) error {
	c.record("DeleteGo")
	return c.DeleteGoErr
}

func (c *Channel) DeleteSummaries(_ context.Context, _ *dfaasv1.LoadTest) error {
	c.record("DeleteSummaries")
	return c.DeleteSummariesErr
}

// GoURL and SummaryURL follow the production precedence (detected address,
// else Public, else ""). Like the Filer's, they are getters, not operations:
// they are not recorded in Calls.
func (c *Channel) GoURL(lt *dfaasv1.LoadTest, g k6dispatch.Generator) string {
	base := c.vmBase(g)
	if base == "" {
		return ""
	}
	return base + syncchannel.GoPath(lt)
}

func (c *Channel) SummaryURL(lt *dfaasv1.LoadTest, g k6dispatch.Generator) string {
	base := c.vmBase(g)
	if base == "" {
		return ""
	}
	return base + syncchannel.SummaryPath(lt, g.NodeID)
}

// vmBase mirrors Filer.vmBase, with Public as the fallback.
func (c *Channel) vmBase(g k6dispatch.Generator) string {
	if g.MgmtAddr != "" {
		return monitoring.FilerPublicBase(g.MgmtAddr)
	}
	return strings.TrimRight(c.Public, "/")
}

func (c *Channel) InClusterSummaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	return "http://filer.test" + syncchannel.SummaryPath(lt, nodeID)
}
