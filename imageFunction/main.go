// Command handler is a tiny OpenFaaS function (of-watchdog http mode) that
// takes a raw image in the request body, converts it to grayscale, and
// downscales it to a thumbnail. It is the load-test workload deployed onto
// dfaas-worker nodes: a real-but-cheap image-processing task.
//
// Wire model: of-watchdog (fwatchdog) listens on :8080, starts this process
// once (fprocess) and proxies each request to upstream_url=:8082. This server
// therefore listens on :8082 and stays resident — no fork-per-request — so it
// behaves well under sustained k6 load.
//
// Input  : raw image bytes (jpeg/png/gif) as the POST body. Matches the k6
//
//	payload feature, which fetches the asset from MinIO and POSTs the
//	bytes directly — no base64, no URL indirection.
//
// Output : image/jpeg thumbnail (default). With ?meta=1 returns a small JSON
//
//	{format,srcWidth,srcHeight,dstWidth,dstHeight} instead, to stress
//	the agent/network rather than the response path.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	// Register decoders for the formats we accept on input.
	_ "image/gif"
	_ "image/png"

	"golang.org/x/image/draw"
)

// defaultThumbSize is the longest-side cap (px) when no override is supplied.
const defaultThumbSize = 128

// maxBodyBytes caps the decoded request body (matches the gateway's 32 MB
// upload limit) so a hostile payload cannot exhaust memory.
const maxBodyBytes = 32 << 20

func main() {
	port := getenv("UPSTREAM_PORT", "8082")

	mux := http.NewServeMux()
	mux.HandleFunc("/", handle)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Printf("image-processor listening on :%s (thumb=%dpx)", port, sizeFromEnv())
	log.Fatal(srv.ListenAndServe())
}

func handle(w http.ResponseWriter, r *http.Request) {
	// GET = liveness/usage probe (also what a browser hit gets).
	if r.Method == http.MethodGet {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("image-processor: POST an image body to grayscale+thumbnail it\n"))
		return
	}

	body, err := readBody(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	src, format, err := image.Decode(bytes.NewReader(body))
	if err != nil {
		// Diagnostic: dump what actually arrived so we can tell a raw image
		// (starts FF D8 / 89 50 4E 47) from a mangled body (e.g. the literal
		// "[object ArrayBuffer]", UTF-8-re-encoded binary starting EF BF BD,
		// base64 text, or a gzip header 1F 8B). Visible via `kubectl logs`.
		preview := body
		if len(preview) > 48 {
			preview = preview[:48]
		}
		log.Printf("decode failed: err=%v bodyLen=%d contentType=%q contentLength=%q contentEncoding=%q first48hex=%x first48str=%q",
			err, len(body), r.Header.Get("Content-Type"), r.Header.Get("Content-Length"),
			r.Header.Get("Content-Encoding"), preview, preview)
		http.Error(w, "decode image: "+err.Error(), http.StatusBadRequest)
		return
	}

	size := thumbSize(r)
	dst := grayThumbnail(src, size)
	sb, db := src.Bounds(), dst.Bounds()

	if r.URL.Query().Get("meta") == "1" {
		writeMeta(w, format, sb, db)
		return
	}

	var out bytes.Buffer
	if err := jpeg.Encode(&out, dst, &jpeg.Options{Quality: 80}); err != nil {
		http.Error(w, "encode jpeg: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	w.Header().Set("X-Src-Format", format)
	w.Header().Set("X-Thumb-Size", strconv.Itoa(size))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out.Bytes())
}

// grayThumbnail scales src so its longest side is <= size, writing straight
// into an *image.Gray. Drawing into a Gray destination does the resize AND the
// colour->luminance conversion in a single pass — no intermediate RGBA buffer.
// ApproxBiLinear is the cheapest scaler, the right call for a load workload.
func grayThumbnail(src image.Image, size int) *image.Gray {
	b := src.Bounds()
	dw, dh := scaledDims(b.Dx(), b.Dy(), size)
	dst := image.NewGray(image.Rect(0, 0, dw, dh))
	draw.ApproxBiLinear.Scale(dst, dst.Bounds(), src, b, draw.Over, nil)
	return dst
}

// scaledDims returns target dimensions preserving aspect ratio, capping the
// longest side at max. Never returns a zero dimension.
func scaledDims(w, h, max int) (int, int) {
	if w <= 0 || h <= 0 {
		return 1, 1
	}
	if w <= max && h <= max {
		return w, h
	}
	if w >= h {
		return max, atLeast1(int(float64(h)*float64(max)/float64(w) + 0.5))
	}
	return atLeast1(int(float64(w)*float64(max)/float64(h) + 0.5)), max
}

func atLeast1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}

func writeMeta(w http.ResponseWriter, format string, src, dst image.Rectangle) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"format":    format,
		"srcWidth":  src.Dx(),
		"srcHeight": src.Dy(),
		"dstWidth":  dst.Dx(),
		"dstHeight": dst.Dy(),
	})
}

// readBody reads the (size-capped) request body and rejects an empty one.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(r.Body); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if buf.Len() == 0 {
		return nil, fmt.Errorf("empty body: POST an image")
	}
	return buf.Bytes(), nil
}

// thumbSize resolves the thumbnail cap: ?size= query wins, then THUMB_SIZE env,
// then the default. Bounded to [1,1024] to keep cost predictable.
func thumbSize(r *http.Request) int {
	if n, ok := parseSize(r.URL.Query().Get("size")); ok {
		return n
	}
	return sizeFromEnv()
}

func sizeFromEnv() int {
	if n, ok := parseSize(os.Getenv("THUMB_SIZE")); ok {
		return n
	}
	return defaultThumbSize
}

func parseSize(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 1024 {
		return 0, false
	}
	return n, true
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
