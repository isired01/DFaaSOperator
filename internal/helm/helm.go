// Package helm wraps the Helm Go SDK to install or upgrade a Helm release
// programmatically from within a Kubernetes Operator reconcile loop.
//
// Design notes:
//   - KubeConfig resolution is delegated to helm.sh/helm/v3/pkg/cli.New(),
//     which auto-detects in-cluster config (when the operator runs as a Pod)
//     and falls back to $KUBECONFIG / ~/.kube/config for local development.
//   - The release storage driver is hard-coded to "secret" (Helm 3 default).
//     Using HELM_DRIVER from the environment is intentionally avoided so the
//     operator's behavior is deterministic across deployment environments.
//   - Install/Upgrade run with Wait=false: the reconciler should not block on
//     readiness inside a single call. The caller is expected to poll the
//     cluster state and requeue.
package helm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
)

// InstallOrUpgradeFromArchive installs the release if it does not exist,
// otherwise upgrades it. The chart is loaded directly from a .tgz/.tar.gz
// io.Reader (typically a bytes.Reader over a go:embed []byte) — the operator
// only ever ships vendored archives, so there is no chart-path/repo variant.
//
// Parameters:
//   - releaseName: Helm release name (must be DNS-1123 compliant).
//   - archive: the packaged chart.
//   - namespace: target namespace for the release. Created if missing on
//     first install.
//   - values: dynamic value overrides, equivalent to `--set` / `-f values.yaml`.
//
// Errors are wrapped with %w so callers can use errors.Is / errors.As. No
// panics are emitted by this package.
func InstallOrUpgradeFromArchive(
	ctx context.Context,
	log logr.Logger,
	releaseName string,
	archive io.Reader,
	namespace string,
	values map[string]interface{},
) (*release.Release, error) {

	cfg, err := newActionConfig(namespace, log)
	if err != nil {
		return nil, fmt.Errorf("init helm action config: %w", err)
	}

	ch, err := loader.LoadArchive(archive)
	if err != nil {
		return nil, fmt.Errorf("load chart archive: %w", err)
	}

	return runInstallOrUpgrade(ctx, log, cfg, ch, releaseName, "<archive>", namespace, values)
}

// runInstallOrUpgrade is the shared install/upgrade branch behind the archive
// entry point.
func runInstallOrUpgrade(
	ctx context.Context,
	log logr.Logger,
	cfg *action.Configuration,
	ch *chart.Chart,
	releaseName, chartLabel, namespace string,
	values map[string]interface{},
) (*release.Release, error) {

	get := action.NewGet(cfg)
	current, err := get.Run(releaseName)
	if err != nil {
		if !errors.Is(err, driver.ErrReleaseNotFound) {
			return nil, fmt.Errorf("lookup release %q: %w", releaseName, err)
		}

		log.Info("Helm install", "release", releaseName, "namespace", namespace, "chart", chartLabel)

		inst := action.NewInstall(cfg)
		inst.ReleaseName = releaseName
		inst.Namespace = namespace
		inst.CreateNamespace = true
		inst.Wait = false

		rel, err := inst.RunWithContext(ctx, ch, values)
		if err != nil {
			return nil, fmt.Errorf("install release %q: %w", releaseName, err)
		}
		return rel, nil
	}

	// A call interrupted mid-flight (operator pod killed, context cancelled)
	// leaves the release parked in a pending-* status, and Helm then refuses
	// every later Upgrade with "another operation (install/upgrade/rollback) is
	// in progress" — forever, since nothing clears that state on its own and
	// the operator never runs `helm rollback`. Unwedge it by marking the stuck
	// revision failed, which is what an Upgrade over a failed release expects.
	if err := recoverPendingRelease(cfg, log, current); err != nil {
		return nil, err
	}

	log.Info("Helm upgrade", "release", releaseName, "namespace", namespace, "chart", chartLabel)

	up := action.NewUpgrade(cfg)
	up.Namespace = namespace
	up.Wait = false
	up.MaxHistory = 5

	rel, err := up.RunWithContext(ctx, releaseName, ch, values)
	if err != nil {
		return nil, fmt.Errorf("upgrade release %q: %w", releaseName, err)
	}
	return rel, nil
}

// recoverPendingRelease clears a release stuck in one of the pending-*
// statuses by marking its latest revision Failed in the storage driver. No-op
// for any other status. Mirrors what an operator would do by hand
// (`helm rollback` / deleting the pending release Secret) — without it, one
// interrupted call wedges every subsequent upgrade of that release.
func recoverPendingRelease(cfg *action.Configuration, log logr.Logger, rel *release.Release) error {
	if rel == nil || rel.Info == nil || !isPendingStatus(rel.Info.Status) {
		return nil
	}
	log.Info("Helm release stuck in a pending status; marking it failed",
		"release", rel.Name, "revision", rel.Version, "status", rel.Info.Status.String())

	rel.Info.Status = release.StatusFailed
	rel.Info.Description = "operation interrupted; marked failed by dfaas-operator"
	if err := cfg.Releases.Update(rel); err != nil {
		return fmt.Errorf("recover pending release %q (revision %d): %w", rel.Name, rel.Version, err)
	}
	return nil
}

// isPendingStatus reports whether a release status means "an operation was
// started and never finished".
func isPendingStatus(s release.Status) bool {
	return s == release.StatusPendingInstall ||
		s == release.StatusPendingUpgrade ||
		s == release.StatusPendingRollback
}

// helmRequestTimeout caps every Kubernetes API request Helm issues. cli.New()
// leaves rest.Config.Timeout at zero, so an unresponsive API server would block
// the reconcile worker on the OS-default TCP behaviour instead of failing —
// same reason the remote k6 dispatcher sets one.
const helmRequestTimeout = 30 * time.Second

// timeoutRESTClientGetter decorates Helm's RESTClientGetter so every REST
// config it hands out carries helmRequestTimeout.
type timeoutRESTClientGetter struct {
	genericclioptions.RESTClientGetter
	timeout time.Duration
}

func (g timeoutRESTClientGetter) ToRESTConfig() (*rest.Config, error) {
	cfg, err := g.RESTClientGetter.ToRESTConfig()
	if err != nil {
		return nil, err
	}
	cfg.Timeout = g.timeout
	return cfg, nil
}

// newActionConfig builds a Helm action.Configuration tied to the given
// namespace, wiring the controller-runtime logger to Helm's internal debug
// hook at log verbosity level 1.
func newActionConfig(namespace string, log logr.Logger) (*action.Configuration, error) {
	settings := cli.New()
	settings.SetNamespace(namespace)

	cfg := new(action.Configuration)
	if err := cfg.Init(
		timeoutRESTClientGetter{settings.RESTClientGetter(), helmRequestTimeout},
		namespace,
		"secret",
		func(format string, v ...interface{}) {
			log.V(1).Info(fmt.Sprintf(format, v...))
		},
	); err != nil {
		return nil, err
	}
	return cfg, nil
}
