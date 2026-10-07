# dfaas-imgproc

A small OpenFaaS function for load tests. It takes a raw image as the request
body, converts it to grayscale, scales it down to a thumbnail and returns a
JPEG: a real but cheap image-processing workload to deploy on the DFaaS nodes
(`dfaas-worker` role) of an Environment.

**Image:** `ghcr.io/isired01/dfaas-imgproc:<version>`, multi-arch
(amd64/arm64). It is not published by `release.yml`: the function almost never
changes, so it is built and pushed by hand (see [Build and push](#build-and-push))
when this directory changes. The published tag is the one pushed last.

Design:

- It runs under of-watchdog in **http mode**: the handler is one resident
  process, not a fork per request, which holds up under sustained k6 load.
- It takes **raw bytes**, which is what the generated k6 scripts POST when a
  scenario has an image payload.
- It is **pure Go** (`golang.org/x/image/draw` and the standard `image/*`
  packages): a static binary, no CGo, low CPU and memory use.

## API

| Request                                 | Response                                                                            |
| --------------------------------------- | ----------------------------------------------------------------------------------- |
| `POST /` with image bytes (jpeg/png/gif) | `image/jpeg` grayscale thumbnail                                                    |
| `POST /?meta=1`                         | JSON `{format,srcWidth,srcHeight,dstWidth,dstHeight}`, a tiny response              |
| `POST /?size=N`                         | override the longest-side cap (1 to 1024 px) for this request                       |
| `GET /`                                 | usage and liveness probe                                                            |

Environment variables: `THUMB_SIZE` (default `128`, longest-side cap) and
`UPSTREAM_PORT` (default `8082`, must match `upstream_url`). The thumbnail keeps
the aspect ratio (400x300 becomes 128x96) and is a single-channel JPEG.

**Input limits.** The request body is capped
at 32 MB (`MaxBytesReader`), which bounds the compressed upload. That is not
enough on its own: a small PNG can declare enormous dimensions, and
`image.Decode` allocates a pixel buffer sized from the header before it reads
any pixel data, which can OOM-kill the pod. So the function reads the header
with `image.DecodeConfig` first and rejects anything above 16 MPx
(`maxImagePixels`) with HTTP 413. A function killed mid-test produces request
failures that look like load-induced errors and contaminate the results.

## Build and push

From the repository root:

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/isired01/dfaas-imgproc:<tag> \
  -f imageFunction/Dockerfile imageFunction --push
```

`--push` is required for a multi-arch build (`--load` cannot hold several
platforms). The function has its own image and module, separate from the
operator, like `dataExporter/`.

## Deploy through the operator

Add it to the `functions` of a `dfaas-worker` node in an Environment. The
operator deploys it with `faas_deploy` and sets the `dfaas.maxrate` and
`dfaas.timeout_ms` labels from `maxRate` and `timeoutMs`:

```yaml
functions:
  - name: imgproc
    image: ghcr.io/isired01/dfaas-imgproc:<version>
    maxRate: 100        # requests per second, used by recalcstrategy
    timeoutMs: 6000
    execTimeout: 5
    maxInflight: 400
```

Then point a k6 scenario at `/function/imgproc` and attach an image payload.
k6 POSTs the bytes and the function returns the thumbnail.

## How a load test delivers the image

The image attached to a scenario in the UI is uploaded to the in-cluster
SeaweedFS, and the generated k6 script delivers it to this function:

1. In k6 `setup()`, which runs once per runner, the script fetches the image
   and base64-encodes it, because binary data does not survive the
   serialization of setup data. If the fetch fails, the script aborts the test
   and names the scenario, the URL it tried and the HTTP status.
2. Each VU decodes it once and POSTs the raw bytes as the request body. The
   object store is therefore read once per runner, not once per VU.
3. The function returns the grayscale thumbnail.

**Use a small image (kilobytes, not megabytes).** The base64 payload from
`setup()` is copied to every VU, so a large image multiplied by many VUs takes a
lot of runner memory. The function also receives the whole image on every
request, so payload size and request rate together load the generator, the
network and the DFaaS node. An oversized payload or an excessive rate shows up
as `500` and `504` responses from a saturated node. Check the failure rate of
the load before comparing runs that used different payloads.

**Reachability.** The runner fetches the image from the SeaweedFS S3 NodePort
`30900`. For an image on the in-cluster SeaweedFS it uses `DFAAS_ASSET_BASE`
when the operator set it (the management address detected for that generator
during provisioning); otherwise it uses the URL the UI gateway baked in at
upload time (`SEAWEEDFS_PUBLIC_URL` on the gateway, else a cluster node's
address), which must then be reachable from that generator VM. The request goes
to the DFaaS node's HAProxy entrypoint,
`http://<dfaas-node-ip>:30080/function/<name>`.

## Functions for smoke tests

Two functions from the OpenFaaS store need no image payload and are useful to
check deployment, routing and agent behavior before sending images to
`imgproc`. Both are pure-Go classic-watchdog functions, multi-arch, and take a
plain-text body. The images are pinned by commit:

| Function | Image                                                                   | Does                          |
| -------- | ----------------------------------------------------------------------- | ----------------------------- |
| figlet   | `ghcr.io/openfaas/figlet:71dcfd7270ac4a47d5b54c05affc63e613277c84`      | text body to an ASCII banner  |
| shasum   | `ghcr.io/openfaas/shasum:71dcfd7270ac4a47d5b54c05affc63e613277c84`      | body to a SHA checksum        |

```yaml
functions:
  - name: figlet
    image: ghcr.io/openfaas/figlet:71dcfd7270ac4a47d5b54c05affc63e613277c84
    maxRate: 100
    timeoutMs: 6000
  - name: shasum
    image: ghcr.io/openfaas/shasum:71dcfd7270ac4a47d5b54c05affc63e613277c84
    maxRate: 100
    timeoutMs: 6000
```

```bash
curl http://<dfaas-node-ip>:30080/function/figlet -d "dfaas"    # banner
curl http://<dfaas-node-ip>:30080/function/shasum -d "dfaas"    # checksum
```

A k6 scenario can send text to `/function/figlet` for a CPU-light baseline, then
switch to `/function/imgproc` for the image-processing load.

## Local test

```bash
UPSTREAM_PORT=8099 THUMB_SIZE=128 go run .
curl -s --data-binary @photo.jpg 'http://127.0.0.1:8099/?meta=1'
curl -s --data-binary @photo.jpg http://127.0.0.1:8099/ -o out.jpg   # grayscale thumbnail
```

The handler on its own listens on `UPSTREAM_PORT`; of-watchdog (port 8080 in the
image) is not involved in this local run.
