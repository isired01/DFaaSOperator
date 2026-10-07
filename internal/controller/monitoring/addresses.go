/*
Copyright 2026 Isaia Del Rosso.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package monitoring

import (
	"fmt"
	"net"
	"strconv"
)

// Where the one SeaweedFS instance is. These live in this package because this
// is where the Helm values that create it live (values/seaweedfs-values.yaml,
// embedded) -- so seaweedfs_values_test.go can pin the values file against
// them.
//
// Before this, the filer NodePort was a literal in three places that could not
// check each other: the reconciler's own const, the values file, and the values
// test's own hardcoded copy. The test could not reference the reconciler's
// const, because controller imports monitoring and the check would have been an
// import cycle -- so the one fact lived on the side of the package split that
// forbade verifying it. Change the values file and the test together and the
// operator silently hands every runner a 404 GO URL: the Sync barrier holds at
// SyncReady=False/AwaitingRunners for five minutes, then Failed/SyncTimeout.
const (
	// SeaweedFSService is the in-cluster Service of the allInOne release.
	SeaweedFSService = "seaweedfs-all-in-one.monitoring.svc.cluster.local"

	// S3Port and FilerPort are the container ports on that Service.
	S3Port    = 8333
	FilerPort = 8888

	// S3NodePort and FilerNodePort are what reaches the same ports from
	// outside the Management cluster: the k6 VMs fetch assets over the S3
	// NodePort and poll the GO signal over the filer NodePort.
	S3NodePort    = 30900
	FilerNodePort = 30901
)

// FilerInClusterBase is the filer base URL from inside the Management cluster.
func FilerInClusterBase() string {
	return fmt.Sprintf("http://%s:%d", SeaweedFSService, FilerPort)
}

// S3InClusterEndpoint is the S3 gateway endpoint from inside the Management
// cluster.
func S3InClusterEndpoint() string {
	return fmt.Sprintf("http://%s:%d", SeaweedFSService, S3Port)
}

// FilerPublicBase is the filer base URL from outside the Management cluster,
// for a client that reaches the Management node at host (an IP or a name, an
// IPv6 literal unbracketed): the filer NodePort on that address. The k6
// runners poll the GO signal and upload their summaries through it.
func FilerPublicBase(host string) string {
	return "http://" + net.JoinHostPort(host, strconv.Itoa(FilerNodePort))
}

// S3PublicBase is the S3 gateway base URL from outside the Management cluster,
// for a client that reaches the Management node at host: the S3 NodePort on
// that address. The k6 runners fetch their payload assets through it.
func S3PublicBase(host string) string {
	return "http://" + net.JoinHostPort(host, strconv.Itoa(S3NodePort))
}
