/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package monitoring

import "testing"

// The VM-facing bases are built on an address a generator dials back, which
// can be an IPv6 literal: a bare "http://fd7a::11:30901" is not a URL the k6
// runner can parse, so the host must be bracketed.
func TestPublicBases(t *testing.T) {
	cases := []struct {
		host      string
		wantFiler string
		wantS3    string
	}{
		{"100.64.0.11", "http://100.64.0.11:30901", "http://100.64.0.11:30900"},
		{"fd7a:115c:a1e0::11", "http://[fd7a:115c:a1e0::11]:30901", "http://[fd7a:115c:a1e0::11]:30900"},
		{"mgmt.lab.example", "http://mgmt.lab.example:30901", "http://mgmt.lab.example:30900"},
	}
	for _, c := range cases {
		if got := FilerPublicBase(c.host); got != c.wantFiler {
			t.Errorf("FilerPublicBase(%q) = %q, want %q", c.host, got, c.wantFiler)
		}
		if got := S3PublicBase(c.host); got != c.wantS3 {
			t.Errorf("S3PublicBase(%q) = %q, want %q", c.host, got, c.wantS3)
		}
	}
	// The ports come from the constants seaweedfs_values_test.go pins to the
	// Helm values, not from a copy here.
	if FilerNodePort != 30901 || S3NodePort != 30900 {
		t.Errorf("NodePorts = %d/%d; the table above expects 30901/30900", FilerNodePort, S3NodePort)
	}
}
