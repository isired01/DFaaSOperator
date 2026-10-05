package monitoring

import (
	"encoding/json"
	"strings"
	"testing"
)

// The live dashboard is provisioned from a file, not created through the
// Grafana API, because Grafana runs with persistence disabled: anything the
// API creates lives in a SQLite file on an emptyDir and is gone with the pod.
// A file-provisioned dashboard is rebuilt identically on every start.
func TestLiveDashboardIsValidAndDynamic(t *testing.T) {
	var dash map[string]any
	if err := json.Unmarshal(liveDashboardJSON, &dash); err != nil {
		t.Fatalf("dashboard JSON does not parse: %v", err)
	}
	if dash["uid"] != liveDashboardUID {
		t.Errorf("uid = %v, want %q — every saved /d/<uid> link would break", dash["uid"], liveDashboardUID)
	}

	// Template variables are what make one dashboard answer both questions:
	// the federation as a whole, or one node.
	tmpl, _ := dash["templating"].(map[string]any)
	list, _ := tmpl["list"].([]any)
	got := map[string]map[string]any{}
	for _, v := range list {
		if m, ok := v.(map[string]any); ok {
			got[m["name"].(string)] = m
		}
	}
	for _, name := range []string{"datasource", "environment", "node", "function"} {
		if _, ok := got[name]; !ok {
			t.Errorf("missing template variable %q", name)
		}
	}
	for _, name := range []string{"node", "function"} {
		v := got[name]
		if v == nil {
			continue
		}
		if v["includeAll"] != true || v["multi"] != true {
			t.Errorf("variable %q must be multi + includeAll, so one panel serves all nodes or one", name)
		}
	}

	panels, _ := dash["panels"].([]any)
	if len(panels) == 0 {
		t.Fatal("dashboard has no panels")
	}
	// Every query must filter on the variables, or "one node" silently shows
	// the whole federation.
	for _, p := range panels {
		panel, _ := p.(map[string]any)
		if panel["type"] == "row" {
			continue
		}
		targets, _ := panel["targets"].([]any)
		if len(targets) == 0 {
			t.Errorf("panel %v has no query", panel["title"])
			continue
		}
		for _, tg := range targets {
			target, _ := tg.(map[string]any)
			expr, _ := target["expr"].(string)
			if !strings.Contains(expr, "$environment") {
				t.Errorf("panel %v: query does not filter by environment: %s", panel["title"], expr)
			}
			if !strings.Contains(expr, "$node") {
				t.Errorf("panel %v: query does not filter by node: %s", panel["title"], expr)
			}
		}
	}
}
