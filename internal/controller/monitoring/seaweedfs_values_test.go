package monitoring

import (
	"testing"

	sigsyaml "sigs.k8s.io/yaml"
)

// TestSeaweedFSValues guards the settings of the embedded SeaweedFS override
// values that are load-bearing elsewhere and would otherwise fail silently:
//
//   - allInOne on and master/volume/filer off. Those three templates are guarded
//     only by their own .enabled, so a true there deploys a whole second
//     StatefulSet topology alongside the all-in-one pod.
//   - the two pinned NodePorts, which remote k6 VMs and the UI gateway hardcode
//     (30900 for the S3 asset URL, 30901 for the filer sync/summary channel).
//   - the admin credentials, which must equal access_key_id/secret_access_key in
//     EnsureDefaultS3Config (internal/controller/s3_bootstrap.go — that package
//     imports this one, so the assertion cannot reference it directly). A drift
//     leaves the exporter Job and the gateway with credentials SeaweedFS rejects.
func TestSeaweedFSValues(t *testing.T) {
	var v struct {
		Master struct{ Enabled bool } `json:"master"`
		Volume struct{ Enabled bool } `json:"volume"`
		Filer  struct{ Enabled bool } `json:"filer"`
		S3     struct {
			Credentials struct {
				Admin struct {
					AccessKey string `json:"accessKey"`
					SecretKey string `json:"secretKey"`
				} `json:"admin"`
			} `json:"credentials"`
		} `json:"s3"`
		AllInOne struct {
			Enabled bool `json:"enabled"`
			S3      struct {
				Enabled    bool `json:"enabled"`
				Port       int  `json:"port"`
				EnableAuth bool `json:"enableAuth"`
			} `json:"s3"`
			Service struct {
				Type      string `json:"type"`
				NodePorts struct {
					S3    int `json:"s3"`
					Filer int `json:"filer"`
				} `json:"nodePorts"`
			} `json:"service"`
		} `json:"allInOne"`
	}

	if err := sigsyaml.Unmarshal(seaweedfsValuesYAML, &v); err != nil {
		t.Fatalf("parse seaweedfs values: %v", err)
	}

	if !v.AllInOne.Enabled {
		t.Error("allInOne.enabled must be true: the sink is the single-process topology")
	}
	for _, c := range []struct {
		name    string
		enabled bool
	}{
		{"master", v.Master.Enabled},
		{"volume", v.Volume.Enabled},
		{"filer", v.Filer.Enabled},
	} {
		if c.enabled {
			t.Errorf("%s.enabled must be false, else the chart also deploys its StatefulSet", c.name)
		}
	}

	if !v.AllInOne.S3.Enabled || !v.AllInOne.S3.EnableAuth {
		t.Error("allInOne.s3 must be enabled with auth, else no S3 gateway and no config Secret")
	}
	if v.AllInOne.S3.Port != S3Port {
		t.Errorf("allInOne.s3.port = %d, want %d", v.AllInOne.S3.Port, S3Port)
	}
	if v.AllInOne.Service.Type != "NodePort" {
		t.Errorf("allInOne.service.type = %q, want NodePort", v.AllInOne.Service.Type)
	}
	// These two are now pinned against the package's own constants, which is
	// what every consumer builds its URLs from -- the values file and the code
	// can finally disagree loudly instead of silently.
	if got := v.AllInOne.Service.NodePorts.S3; got != S3NodePort {
		t.Errorf("S3 nodePort = %d, want %d (asset URL handed to k6)", got, S3NodePort)
	}
	if got := v.AllInOne.Service.NodePorts.Filer; got != FilerNodePort {
		t.Errorf("filer nodePort = %d, want %d (syncStart GO signal + k6 summaries)", got, FilerNodePort)
	}

	admin := v.S3.Credentials.Admin
	if admin.AccessKey != "admin" || admin.SecretKey != "admin123" {
		t.Errorf("s3.credentials.admin = %q/%q, want admin/admin123 to match EnsureDefaultS3Config",
			admin.AccessKey, admin.SecretKey)
	}
}
