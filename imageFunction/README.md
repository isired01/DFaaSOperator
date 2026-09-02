# dfaas-imgproc — image-processing load-test function

Tiny OpenFaaS function (of-watchdog **http mode**) for the dFaaS load tests:
takes a raw image in the request body, **grayscales + downscales** it to a
thumbnail, returns JPEG. A real-but-cheap image-processing workload deployed
onto `dfaas-worker` nodes.

**Image:** `ghcr.io/isired01/dfaas-imgproc:latest` — multi-arch (amd64/arm64),
available once built and pushed (see [Build & push](#build--push-multi-arch)).

Why this shape:
- **of-watchdog http mode** → the handler is a resident process, not
  fork-per-request — behaves well under sustained k6 load.
- **raw bytes in** → matches the k6 payload feature, which fetches the asset
  from SeaweedFS and POSTs the bytes directly (no base64, no URL).
- **pure Go** (`golang.org/x/image/draw`, stdlib `image/*`) → static binary,
  no OpenCV/CGo, multi-arch, low CPU/memory.

## API

| | |
|---|---|
| `POST /` body = image bytes (jpeg/png/gif) | → `image/jpeg` grayscale thumbnail |
| `POST /?meta=1` | → JSON `{format,srcWidth,srcHeight,dstWidth,dstHeight}` (tiny response — stress agent/network, not the response path) |
| `POST /?size=N` | override longest-side cap (1–1024 px) for this request |
| `GET /` | usage/liveness probe |

Tuning env: `THUMB_SIZE` (default `128`, longest-side cap), `UPSTREAM_PORT`
(default `8082`, must match `upstream_url`).

**Input limits — two, guarding different things.** The request body is capped at
32 MB (`MaxBytesReader`), which bounds the *compressed* upload. That alone is not
enough: a small, well-formed PNG can declare enormous dimensions, and Go's
`image.Decode` allocates a pixel buffer sized to the declared header before
reading any pixel data — a decompression bomb that OOM-kills the pod. So the
header is parsed first with `image.DecodeConfig` and anything above
`maxImagePixels` (16 MPx) is rejected with **413** before decoding. This matters
under load: a function OOM-killed mid-experiment shows up as request failures
that look exactly like load-induced errors, quietly contaminating the results.

Output JPEG is single-channel (`components 1`) grayscale; aspect ratio
preserved (e.g. 400×300 → 128×96).

## Build & push (multi-arch)

```bash
docker buildx build --platform linux/amd64,linux/arm64 \
  -t ghcr.io/isired01/dfaas-imgproc:latest -f Dockerfile . --push
```

`--push` is mandatory for multi-arch (no `--load`). Lives outside the operator
Dockerfile, same as `dataExporter/`.

## Deploy via the operator

Add it to an `Environment`'s `dfaas-worker` node `functions[]` — the operator
deploys it through `faas_deploy` and stamps the `dfaas.maxrate` /
`dfaas.timeout_ms` labels:

```yaml
functions:
  - name: imgproc
    image: ghcr.io/isired01/dfaas-imgproc:latest
    maxRate: 100        # req/s cap consumed by recalcstrategy
    timeoutMs: 6000
    execTimeout: 5
    maxInflight: 400
```

Then point a k6 scenario at `/function/imgproc` and attach an image payload —
k6 POSTs the bytes, the function returns the grayscale thumbnail.

## Load-test payload flow (how k6 sends the image)

The image attached to a k6 scenario in the UI is uploaded to the in-cluster SeaweedFS
(S3) and the generated k6 script delivers it to this function like so:

1. The script fetches the image **once** in k6 `setup()` (runs a single time per
   test) and base64-encodes it — binary can't survive setup-data serialization.
2. Each VU base64-decodes it **once** and POSTs the **raw bytes** as the request
   body. So SeaweedFS is hit one time regardless of VU count — *not* once per VU.
3. This function reads the raw body and returns the grayscale thumbnail.

**Use a small image (KB, not multi-MB).** Two independent reasons:
- the base64 payload from `setup()` is copied to every VU → a multi-MB image ×
  thousands of VUs is a lot of runner memory;
- the function receives the **full image on every request** → a large image
  saturates the object store / network / DFaaS node under load.

Symptoms of an oversized payload or excessive rate under load:
`400 decode image: unknown format` (a payload fetch failed and a non-image body
slipped through — guarded against in current scripts) and `500/504` (node /
gateway saturated). Keep the arrival rate sane.

Reachability: the asset URL handed to k6 must be reachable **from the k6 VMs**
(`SEAWEEDFS_PUBLIC_URL` on the UI gateway; SeaweedFS S3 API port **30900**); the
target URL is the DFaaS node's OpenFaaS entrypoint
`http://<dfaas-node-ip>:30080/function/<name>` (HAProxy NodePort).

## Ready-made test functions (no payload needed)

Two official OpenFaaS store functions — handy as lightweight smoke tests to
verify deploy / routing / agent behaviour **before** throwing images at
`imgproc`. Both are pure-Go classic-watchdog functions, multi-arch
(amd64/arm64), and take a **plain-text body** (no SeaweedFS asset, no payload
feature). Images pinned by commit:

| Function | Image | Does |
|---|---|---|
| figlet | `ghcr.io/openfaas/figlet:71dcfd7270ac4a47d5b54c05affc63e613277c84` | text body → ASCII-art banner |
| shasum | `ghcr.io/openfaas/shasum:71dcfd7270ac4a47d5b54c05affc63e613277c84` | body → SHA checksum |

Deploy them the same way (node `functions[]`):

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
curl http://<gateway>/function/figlet -d "dfaas"     # banner
curl http://<gateway>/function/shasum -d "dfaas"     # checksum
```

A k6 scenario can hammer `/function/figlet` (text body) for a CPU-light
baseline, then switch to `/function/imgproc` (image payload) for the real
image-processing load.

## Local test

```bash
UPSTREAM_PORT=8099 THUMB_SIZE=128 go run .
curl -s --data-binary @photo.jpg http://127.0.0.1:8099/?meta=1
curl -s --data-binary @photo.jpg http://127.0.0.1:8099/ -o out.jpg   # grayscale thumbnail
```
