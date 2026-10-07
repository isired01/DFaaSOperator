# Releasing

Both repositories are released together, under the same tag, even when one of them did not change: the chart defaults the UI image tag to its own `appVersion`. This document describes the procedure as the two release workflows and the git history show it, then upgrading, uninstalling, and what to change to move the project to another owner.

## What a release publishes

| Repository | Trigger | Published |
| --- | --- | --- |
| DFaaS_UI | a tag matching `v*` | `ghcr.io/isired01/dfaas-control-plane:vX.Y.Z`, `:X.Y.Z` and `:latest`, multi-arch (`linux/amd64`, `linux/arm64`). |
| DFaaSOperator | a tag matching `v*` | The images `dfaas-operator`, `dfaas-exporter` and `dfaas-imgproc` under `vX.Y.Z`, `X.Y.Z` and `latest`, multi-arch. The Helm chart as `oci://ghcr.io/isired01/charts/dfaas` version `X.Y.Z`. A GitHub Release with the packaged chart (`dfaas-X.Y.Z.tgz`) and the two CRD files. |

The images are published under both `vX.Y.Z` and `X.Y.Z` because the chart defaults every image tag to its `appVersion`, which `helm package` sets without the leading `v`.

The chart in the repository is a template: `Chart.yaml` carries the placeholder version `0.0.0` on purpose. `release.yml` packages it with `--version` and `--app-version` taken from the tag, so a checkout install must pass real image tags (`operator.image.tag`, `operator.exporterImage.tag`, `ui.image.tag`).

The operator's `test.yml` also publishes `ghcr.io/isired01/dfaas-exporter:latest` on every push to `main`. `latest` of the exporter therefore means "current main", not "last release".

Nothing in either workflow runs the tests. The release workflows lint the chart (the operator's) and build and push; the tests run in `test.yml` on the pull requests. Tag only a commit whose CI run is green. The only secret the workflows use is the automatic `GITHUB_TOKEN`; the operator's `release.yml` has `contents: write` and `packages: write`, the UI's `packages: write`.

The dfaas-agent image and chart are not released by any workflow ([dfaas-agent.md](dfaas-agent.md)).

## Procedure

Assume the version is `X.Y.Z`. Do each step in both repositories unless it says otherwise.

1. **Settle `main`.** Everything for the release is merged and CI is green in both repositories. If the release changes a CRD, the checklist in [cross-repo-contract.md](cross-repo-contract.md#changing-a-crd-field) is complete and the chart's `crds/` equals `config/crd/bases` (CI checks it).
2. **Bump the pinned versions in the READMEs**, in a pull request merged before the tag: the `--version` and the `--set ...tag=` values in the operator README's install and upgrade commands, and the version in the UI README's upgrade commands (`v4.0.2` in the CRD URLs and `--version 4.0.2`). The history does this as `docs: point the install and upgrade commands at vX.Y.Z`. A version bump that waits until after the tag leaves a tagged tree whose README names the previous release (the UI repository's v4.0.2 was followed by such a pull request). Nothing checks these pins.
3. **Tag the UI repository first**, on the merge commit of `main`:

   ```bash
   git tag -a vX.Y.Z -m vX.Y.Z
   git push origin vX.Y.Z
   ```

   The existing tags are annotated, with the tag name as the message. The UI's `release.yml` builds and pushes `dfaas-control-plane`. It is the slow build (two architectures).
4. **Tag the operator repository** the same way. Its `release.yml` runs in this order:
   1. computes the version without the `v`;
   2. lints the chart (a template error stops the release before anything is pushed);
   3. logs in to `ghcr.io` with `GITHUB_TOKEN`;
   4. builds and pushes the operator, exporter and imgproc images;
   5. **waits up to 15 minutes** (60 tries, 15 s apart) for `ghcr.io/isired01/dfaas-control-plane:vX.Y.Z` to exist, and fails if it never does: that is why the UI tag comes first, and a private `dfaas-control-plane` package also ends the job here;
   6. copies the two CRD files into `dist/`;
   7. packages the chart with `--version X.Y.Z --app-version X.Y.Z`;
   8. pushes the chart to `oci://ghcr.io/isired01/charts`;
   9. creates the GitHub Release with `dist/dfaas-X.Y.Z.tgz` and the two CRD files, with generated release notes.
5. **Verify** from a machine that is not logged in to `ghcr.io`:

   ```bash
   docker manifest inspect ghcr.io/isired01/dfaas-operator:X.Y.Z > /dev/null
   docker manifest inspect ghcr.io/isired01/dfaas-exporter:X.Y.Z > /dev/null
   docker manifest inspect ghcr.io/isired01/dfaas-imgproc:X.Y.Z > /dev/null
   docker manifest inspect ghcr.io/isired01/dfaas-control-plane:X.Y.Z > /dev/null
   helm pull oci://ghcr.io/isired01/charts/dfaas --version X.Y.Z --untar --untardir /tmp/dfaas-X.Y.Z
   diff -r /tmp/dfaas-X.Y.Z/dfaas/crds charts/dfaas/crds
   ```

6. **Edit the release body** to open with two lines, because the workflow uses only generated notes: `CRDs changed: yes/no (apply them before helm upgrade)` and `Export format changed: yes/no`.

If the wait for the UI image times out, re-run the failed job from the Actions page once the UI image exists. **Never move or re-push a tag**: a push re-triggers the publish workflow, and the tag moves under people who already pulled it. The history shows `v4.0.0` and `v4.0.1` on the same commit, which is what re-tagging leaves behind. If a published release is wrong, fix it in a new patch release.

## Upgrading

Helm applies the contents of a chart's `crds/` on install only, never on upgrade. Before `helm upgrade` to a chart whose schema changed, apply that chart's CRDs. An operator started against the old CRDs has the new status fields pruned by the API server. The commands are in the README ([Upgrade](../README.md#upgrade)):

```bash
helm pull oci://ghcr.io/isired01/charts/dfaas --version <version> --untar --untardir /tmp/dfaas-<version>
kubectl apply -f /tmp/dfaas-<version>/dfaas/crds/
helm upgrade dfaas oci://ghcr.io/isired01/charts/dfaas --version <version> --namespace dfaas-operator-system
```

From a checkout of the tag, `kubectl apply -f charts/dfaas/crds/` is the same as the pull. The two CRD files are also attached to the GitHub Release, but a release asset URL returns 404 while the repository is private, so the `helm pull` form is the one that does not depend on repository visibility.

Notes that depend on the release being left:

- A release that adds a CRD status field (`status.provisioningGeneration`, `status.k6Nodes[].managementAddress`) is the reason the order matters. An Environment that was `Ready` before the field existed gets its value on its next provisioning run, that is, after a spec edit.
- A release that removes a CRD field (`spec.topology` of the Environment) makes `kubectl apply` reject a manifest that still sets it. Remove the block from your manifests.
- The operator no longer writes the Secret `<env>-provisioned-nodes`; existing ones are garbage-collected with their Environment.
- The management Prometheus restarts once, at the next provisioning of an Environment after an upgrade that changes its pod spec.
- A management cluster bootstrapped before v2.5, which still has the SeaweedFS objects of the pre-Helm install (the Service `seaweedfs` and so on), is no longer migrated automatically. Upgrade it through a release that still contains the migration first.

## Uninstalling

Both resources carry finalizers that only the running operator removes. Delete them before removing the operator.

```bash
# 1. While the operator is still running
kubectl delete loadtests.dfaas.dfaas.io --all -A
kubectl delete environments.dfaas.dfaas.io --all -A    # wait until they are gone

# 2. The chart (also removes the dfaas-ui namespace)
helm uninstall dfaas -n dfaas-operator-system
kubectl delete namespace dfaas-operator-system         # if created with --create-namespace

# 3. What the operator installed at run time. SeaweedFS holds every exported result: copy what you need first.
helm uninstall -n monitoring prometheus grafana seaweedfs
kubectl delete namespace monitoring dfaas-s3

# 4. The CRDs, which Helm never removes
kubectl delete crd environments.dfaas.dfaas.io loadtests.dfaas.dfaas.io
```

If the operator is already gone and an object hangs in `Terminating`:

```bash
kubectl patch <kind> <name> -n <namespace> --type=merge -p '{"metadata":{"finalizers":null}}'
```

Nothing is removed from the VMs. Run `sudo /usr/local/bin/k3s-uninstall.sh` on each one to wipe it.

## Moving the project to another owner

Everything published sits under the GitHub user `isired01`. The owner is written into the repositories in the places below. `git grep -n isired01` in both repositories is the authoritative list; run it after you change these, and expect the docs themselves to match (they name the published locations).

### DFaaSOperator

| Path | What |
| --- | --- |
| `.github/workflows/release.yml` | The image names `ghcr.io/isired01/dfaas-operator`, `dfaas-exporter`, `dfaas-imgproc`; the wait for `ghcr.io/isired01/dfaas-control-plane`; the chart push to `oci://ghcr.io/isired01/charts`. |
| `.github/workflows/test.yml` | The `publish-exporter` job's tag `ghcr.io/isired01/dfaas-exporter:latest`. |
| `charts/dfaas/values.yaml` | The default image repositories of the operator, the exporter and the UI; the `helm pull` example in the comments. |
| `charts/dfaas/Chart.yaml` | `home`, `sources` and `maintainers`. |
| `charts/dfaas/templates/NOTES.txt` | The `helm pull` command it prints. |
| `internal/controller/exporter_job.go` | `exporterImage()`: the fallback image `ghcr.io/isired01/dfaas-exporter:latest` used when `DFAAS_EXPORTER_IMAGE` is unset (a test comment in `exporter_job_name_test.go` mentions it). |
| `internal/controller/ansible/templates/setup-nodes.yml` | The dfaas-agent image (`ghcr.io/isired01/dfaas-agent:dev`) and chart (`oci://ghcr.io/isired01/dfaas-agent-chart`). These live in the owner's registry and no workflow publishes them: see below. |
| `imageFunction/Dockerfile`, `imageFunction/README.md` | The imgproc image name. `imageFunction/go.mod` has the module name `github.com/isired01/dfaas-imgproc`, which is only a name and does not have to change. |
| `README.md`, `docs/` | Install and upgrade commands, links to the UI repository, release URLs. |

The embedded playbooks and `exporter_job.go` are compiled into the operator image, so a change to the agent image or chart reference reaches a machine only through a new operator image and a re-provisioning.

### DFaaS_UI

| Path | What |
| --- | --- |
| `.github/workflows/release.yml` | The image name `ghcr.io/isired01/dfaas-control-plane`. |
| `README.md`, `docs/` | Links to the operator repository and its `docs/`; the install and CRD URLs. |
| `ui/src/components/NodeForm.jsx` | A tooltip that names an example image under `ghcr.io/isired01`. |

### Outside the repositories

- **GHCR packages** and their ownership: `dfaas-operator`, `dfaas-exporter`, `dfaas-imgproc`, `dfaas-control-plane`, `charts/dfaas`, `dfaas-agent` and `dfaas-agent-chart`. A package belongs to an owner namespace, so a new owner publishes new ones. The new owner has to pull what exists and push it again for the two that no workflow builds:
  - the agent image: `docker pull ghcr.io/isired01/dfaas-agent:dev`, retag, push, or rebuild it from upstream ([dfaas-agent.md](dfaas-agent.md));
  - the agent chart, whose source is in no repository: `helm pull oci://ghcr.io/isired01/dfaas-agent-chart --version 0.1.3`, then `helm push` the archive to the new location. Do this while the old packages still exist.
- **Repository settings**: Actions must be enabled, and the workflows request their own `permissions`, so no secret has to be created. The first publish of a package from a workflow needs the package to be linked to the repository, and an existing package must grant the repository write access (package settings, "Manage Actions access"). The imgproc image was pushed by hand before it was part of `release.yml`, so its package needs that grant.
- **Maintainer metadata**: `Chart.yaml` `maintainers`, the license header holder (`Copyright 2026 Isaia Del Rosso` in `LICENSE` and in every Go file, and in `hack/boilerplate.go.txt` for generated code) if the copyright holder changes. The three values files derived from upstream keep their own notice.

## GHCR packages

All images and the chart are pulled without credentials. The operator and UI pods can use `imagePullSecrets` (`imagePullSecrets` in the chart values), but the exporter Jobs and the VMs cannot, so every package named above must be public for an install to work. A release whose `dfaas-control-plane` package is private fails at the wait step. Check visibility under the package's settings on GitHub after the first publish of a new package, and again when the owner changes.
