/*
Copyright 2026.

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
	"sync"

	dfaasv1 "dfaas-operator/api/v1"
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

	// Public is the VM-facing base. Empty means unresolvable, which is what
	// makes GoURL return "" and the dispatch fail loudly.
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

func (c *Channel) GoURL(lt *dfaasv1.LoadTest) string {
	if c.Public == "" {
		return ""
	}
	return c.Public + "/dfaas-sync/" + lt.Namespace + "/" + lt.Name + ".go"
}

func (c *Channel) SummaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	if c.Public == "" {
		return ""
	}
	return c.Public + syncchannel.SummaryPath(lt, nodeID)
}

func (c *Channel) InClusterSummaryURL(lt *dfaasv1.LoadTest, nodeID string) string {
	return "http://filer.test" + syncchannel.SummaryPath(lt, nodeID)
}
