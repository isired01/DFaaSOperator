/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"regexp"
	"strings"
)

// condMessage produces a Condition message string from an error such that the
// same error class always produces the same string. This is required so
// meta.SetStatusCondition keeps LastTransitionTime stable across reconciles —
// raw err.Error() values often interpolate per-request UIDs, pod-instance
// names, timestamps and host:port pairs that mutate every tick, defeating
// the "only update on real transition" optimization.
//
// The helper strips:
//   - RFC3339 timestamps with optional sub-second component.
//   - UUID-style identifiers (8-4-4-4-12 hex).
//   - bare 16/32-char hex blobs (request IDs, pod UIDs).
//   - IPv4 addresses + :port suffixes.
//   - bracketed pod-instance suffixes like "-abcde-12345".
//   - extra runs of whitespace, leading/trailing spaces.
//
// Result is a single line, trimmed, with a hard ceiling of 256 chars so
// pathological errors do not blow up the conditions slice. The aim is
// stability, not perfect prose: callers should add their own static prefix.
func condMessage(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	s = reTimestamp.ReplaceAllString(s, "<ts>")
	s = reUUID.ReplaceAllString(s, "<uid>")
	s = reHexLong.ReplaceAllString(s, "<id>")
	s = reIPPort.ReplaceAllString(s, "<addr>")
	s = rePodSuffix.ReplaceAllString(s, "")
	s = reNewline.ReplaceAllString(s, " ")
	s = reMultiSpace.ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	if len(s) > 256 {
		s = s[:256]
	}
	return s
}

var (
	reTimestamp  = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:?\d{2})?\b`)
	reUUID       = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	reHexLong    = regexp.MustCompile(`\b[0-9a-fA-F]{16,}\b`)
	reIPPort     = regexp.MustCompile(`\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d+)?\b`)
	rePodSuffix  = regexp.MustCompile(`-[a-z0-9]{5,10}-[a-z0-9]{5}\b`)
	reNewline    = regexp.MustCompile(`[\r\n]+`)
	reMultiSpace = regexp.MustCompile(`[ \t]{2,}`)
)
