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

	"github.com/go-logr/logr"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/release"
	"helm.sh/helm/v3/pkg/storage/driver"
)

// InstallOrUpgrade installs the release if it does not exist, otherwise
// upgrades it.
//
// Parameters:
//   - releaseName: Helm release name (must be DNS-1123 compliant).
//   - chartPathOrName: either a local filesystem path ("./monitoring-chart")
//     or a "repo/chart" reference. Repo references require the repo to be
//     present in the local Helm repository configuration
//     (settings.RepositoryConfig, defaulting to ~/.config/helm/repositories.yaml).
//   - namespace: target namespace for the release. Created if missing on
//     first install.
//   - values: dynamic value overrides, equivalent to `--set` / `-f values.yaml`.
//
// Errors are wrapped with %w so callers can use errors.Is / errors.As. No
// panics are emitted by this package.
func InstallOrUpgrade(
	ctx context.Context,
	log logr.Logger,
	releaseName, chartPathOrName, namespace string,
	values map[string]interface{},
) (*release.Release, error) {

	cfg, settings, err := newActionConfig(namespace, log)
	if err != nil {
		return nil, fmt.Errorf("init helm action config: %w", err)
	}

	chartPath, err := (&action.ChartPathOptions{}).LocateChart(chartPathOrName, settings)
	if err != nil {
		return nil, fmt.Errorf("locate chart %q: %w", chartPathOrName, err)
	}

	ch, err := loader.Load(chartPath)
	if err != nil {
		return nil, fmt.Errorf("load chart %q: %w", chartPath, err)
	}

	return runInstallOrUpgrade(ctx, log, cfg, ch, releaseName, chartPathOrName, namespace, values)
}

// InstallOrUpgradeFromArchive is the in-memory variant of InstallOrUpgrade:
// it loads the chart directly from a .tgz/.tar.gz io.Reader (typically a
// bytes.Reader over a go:embed []byte). Same install-vs-upgrade semantics.
func InstallOrUpgradeFromArchive(
	ctx context.Context,
	log logr.Logger,
	releaseName string,
	archive io.Reader,
	namespace string,
	values map[string]interface{},
) (*release.Release, error) {

	cfg, _, err := newActionConfig(namespace, log)
	if err != nil {
		return nil, fmt.Errorf("init helm action config: %w", err)
	}

	ch, err := loader.LoadArchive(archive)
	if err != nil {
		return nil, fmt.Errorf("load chart archive: %w", err)
	}

	return runInstallOrUpgrade(ctx, log, cfg, ch, releaseName, "<archive>", namespace, values)
}

// runInstallOrUpgrade is the shared install/upgrade branch used by both the
// path-based and the archive-based entry points.
func runInstallOrUpgrade(
	ctx context.Context,
	log logr.Logger,
	cfg *action.Configuration,
	ch *chart.Chart,
	releaseName, chartLabel, namespace string,
	values map[string]interface{},
) (*release.Release, error) {

	get := action.NewGet(cfg)
	if _, err := get.Run(releaseName); err != nil {
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

// newActionConfig builds a Helm action.Configuration tied to the given
// namespace, wiring the controller-runtime logger to Helm's internal debug
// hook at log verbosity level 1.
func newActionConfig(namespace string, log logr.Logger) (*action.Configuration, *cli.EnvSettings, error) {
	settings := cli.New()
	settings.SetNamespace(namespace)

	cfg := new(action.Configuration)
	if err := cfg.Init(
		settings.RESTClientGetter(),
		namespace,
		"secret",
		func(format string, v ...interface{}) {
			log.V(1).Info(fmt.Sprintf(format, v...))
		},
	); err != nil {
		return nil, nil, err
	}
	return cfg, settings, nil
}
