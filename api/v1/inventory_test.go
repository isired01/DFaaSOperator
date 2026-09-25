/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The phase and reason inventories are read from the source instead of being
// re-listed by hand, so a constant added without classification, or left
// behind by the code that stamped it, fails here and not in the UI.

type stringConst struct{ name, typ, value string }

func typesFiles(t *testing.T) []*ast.File {
	t.Helper()
	paths, err := filepath.Glob("*_types.go")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no *_types.go files: %v", err)
	}
	var files []*ast.File
	fset := token.NewFileSet()
	for _, p := range paths {
		f, err := parser.ParseFile(fset, p, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

// stringConsts returns every string const in the API types, with its declared
// type ("" when untyped).
func stringConsts(t *testing.T) []stringConst {
	t.Helper()
	var out []stringConst
	for _, f := range typesFiles(t) {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, spec := range gd.Specs {
				vs := spec.(*ast.ValueSpec)
				typ := ""
				if id, ok := vs.Type.(*ast.Ident); ok {
					typ = id.Name
				}
				for i, n := range vs.Names {
					isReason := strings.HasPrefix(n.Name, "EnvReason") || strings.HasPrefix(n.Name, "LTReason")
					var lit *ast.BasicLit
					if i < len(vs.Values) {
						lit, _ = vs.Values[i].(*ast.BasicLit)
					}
					if lit == nil || lit.Kind != token.STRING {
						if isReason {
							t.Fatalf("reason %s has no string literal", n.Name)
						}
						continue
					}
					v, err := strconv.Unquote(lit.Value)
					if err != nil {
						t.Fatal(err)
					}
					out = append(out, stringConst{n.Name, typ, v})
				}
			}
		}
	}
	return out
}

var enumRE = regexp.MustCompile(`\+kubebuilder:validation:Enum=(\S+)`)

// phasesOf returns "" plus the Enum marker values of typeName, after checking
// that the marker and the typed constants declare the same set.
func phasesOf[P ~string](t *testing.T, typeName string) []P {
	t.Helper()
	var marker []string
	for _, f := range typesFiles(t) {
		for _, d := range f.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}
			for _, spec := range gd.Specs {
				ts := spec.(*ast.TypeSpec)
				if ts.Name.Name != typeName {
					continue
				}
				for _, doc := range []*ast.CommentGroup{gd.Doc, ts.Doc} {
					if doc == nil {
						continue
					}
					for _, c := range doc.List {
						if m := enumRE.FindStringSubmatch(c.Text); m != nil {
							marker = strings.Split(m[1], ";")
						}
					}
				}
			}
		}
	}
	if len(marker) == 0 {
		t.Fatalf("%s has no +kubebuilder:validation:Enum marker", typeName)
	}
	var consts []string
	for _, c := range stringConsts(t) {
		if c.typ == typeName && c.value != "" {
			consts = append(consts, c.value)
		}
	}
	sort.Strings(consts)
	sorted := append([]string(nil), marker...)
	sort.Strings(sorted)
	if strings.Join(sorted, ";") != strings.Join(consts, ";") {
		t.Fatalf("%s: Enum marker %v != typed constants %v", typeName, sorted, consts)
	}
	out := []P{""}
	for _, v := range marker {
		out = append(out, P(v))
	}
	return out
}

// terminalFailure: true iff stamping the reason means the object failed for
// good, with no automatic recovery. Keyed by the constants, so deleting one is
// a compile error and a shared string ("Failed", "Skipped") has one row. The
// UI's crstate curation must give every true row the tone "error".
var terminalFailure = map[string]bool{
	EnvReasonAnsibleFailed:  true,
	EnvReasonHelmFailed:     true,
	EnvReasonInfraFailed:    true,
	EnvReasonFailed:         true, // == LTReasonFailed
	LTReasonDispatchFailed:  true,
	LTReasonPartialFailure:  true,
	LTReasonAllFailed:       true,
	LTReasonJobFailed:       true,
	LTReasonSyncTimeout:     true,
	LTReasonS3ConfigMissing: true,
	LTReasonEnvNotFound:     true,

	EnvReasonSkipped:            false, // == LTReasonExportSkipped
	EnvReasonNoWorkers:          false,
	EnvReasonNoK6Nodes:          false,
	EnvReasonVMsProvisioned:     false,
	EnvReasonK6Provisioned:      false,
	EnvReasonSSHReachable:       false,
	EnvReasonSSHUnreachable:     false, // Unreachable re-probes forever
	EnvReasonAnsibleRunning:     false,
	EnvReasonJobCreationFailed:  false, // retried with controller-runtime backoff
	EnvReasonHelmInstalling:     false,
	EnvReasonWaitingPods:        false,
	EnvReasonPodsRunning:        false,
	EnvReasonInfraReady:         false,
	EnvReasonUpdating:           false,
	EnvReasonInitializing:       false,
	EnvReasonProvisioning:       false,
	EnvReasonAllSubsystemsReady: false,
	EnvReasonJobPending:         false,
	EnvReasonCheckFailed:        false, // retried every 10 s

	LTReasonScheduledArmed:         false,
	LTReasonScheduledFired:         false,
	LTReasonScheduledDelayedEnvNot: false,
	LTReasonInFlight:               false,
	LTReasonAllDispatched:          false,
	LTReasonDispatchedUnreachable:  false, // warning on K6Dispatched=True; the test runs on
	LTReasonPending:                false,
	LTReasonAllFinished:            false,
	LTReasonRunning:                false,
	LTReasonExportCooldown:         false,
	LTReasonExporterRunning:        false,
	LTReasonExportSucceeded:        false,
	LTReasonRunnersReclaimed:       false,
	LTReasonRunnersUnreclaimed:     false, // retried in the background
	LTReasonUserAborted:            false, // an abort, not a failure
	LTReasonCompleted:              false,
	LTReasonAborted:                false,
	LTReasonScriptMirrorFailed:     false, // dispatch retry sub-cause
	LTReasonStaleCleanupFailed:     false,
	LTReasonAwaitingRunners:        false,
	LTReasonGoPublished:            false,
	LTReasonApplyFailed:            false,
	LTReasonFetchFailed:            false,
	LTReasonEnvFound:               false,
	LTReasonEnvFailed:              false, // EnvironmentLinked detail; the test fails through its own gate
	LTReasonEnvBusy:                false,
	LTReasonQueuedBehind:           false,
	LTReasonDispatching:            false,
}

func reasonConsts(t *testing.T) []stringConst {
	var out []stringConst
	for _, c := range stringConsts(t) {
		if strings.HasPrefix(c.name, "EnvReason") || strings.HasPrefix(c.name, "LTReason") {
			out = append(out, c)
		}
	}
	return out
}

func TestEveryReasonIsClassified(t *testing.T) {
	for _, c := range reasonConsts(t) {
		if _, ok := terminalFailure[c.value]; !ok {
			t.Errorf("reason %s (%q) is not classified in terminalFailure", c.name, c.value)
		}
	}
}

// TestNoDeadReasons: a reason nothing stamps is a lie in the UI's curation
// table and in the docs. Every constant must be referenced by non-test code.
func TestNoDeadReasons(t *testing.T) {
	used := map[string]bool{}
	for _, root := range []string{"../../internal", "../../cmd"} {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, perr := parser.ParseFile(token.NewFileSet(), p, nil, 0)
			if perr != nil {
				return perr
			}
			ast.Inspect(f, func(n ast.Node) bool {
				if se, ok := n.(*ast.SelectorExpr); ok {
					used[se.Sel.Name] = true
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range reasonConsts(t) {
		if !used[c.name] {
			t.Errorf("reason %s is declared but never stamped", c.name)
		}
	}
}
