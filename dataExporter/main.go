package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
)

// MetricEntry mirrors the operator's MetricExportEntry. The operator
// resolves raw-type metricName defaults before serialization, so each
// entry here always has both metricName and query populated.
type MetricEntry struct {
	Type       string `json:"type"`
	MetricName string `json:"metricName"`
	Query      string `json:"query"`
	Comment    string `json:"comment,omitempty"`
}

func main() {
	// 1. Recupero parametri dalle Env Var (configurate dall'Operator).
	promURL := os.Getenv("PROM_URL")
	startStr := os.Getenv("START_TIME")
	endStr := os.Getenv("END_TIME")
	stepStr := os.Getenv("STEP")
	expName := os.Getenv("EXP_NAME")
	metricsJSON := os.Getenv("METRICS_JSON")

	if metricsJSON == "" {
		log.Fatalf("METRICS_JSON env var is empty")
	}
	var metrics []MetricEntry
	if err := json.Unmarshal([]byte(metricsJSON), &metrics); err != nil {
		log.Fatalf("METRICS_JSON parse error: %v", err)
	}
	if len(metrics) == 0 {
		log.Fatalf("METRICS_JSON contains zero entries")
	}

	// Destinazione opzionale Google Drive (impostata dall'Operator se
	// spec.metricsExport.googleDrive è valorizzata). Se vuote: fallback stdout.
	gdriveFolderID := os.Getenv("GDRIVE_FOLDER_ID")
	gdriveCredPath := os.Getenv("GDRIVE_CREDENTIALS_PATH")

	// 2. Parsing dei tempi e dello step.
	start, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		log.Fatalf("Errore parsing START_TIME %q: %v", startStr, err)
	}
	end, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		log.Fatalf("Errore parsing END_TIME %q: %v", endStr, err)
	}
	step, err := time.ParseDuration(stepStr)
	if err != nil {
		log.Fatalf("Errore parsing STEP %q: %v", stepStr, err)
	}

	// 3. Setup Client Prometheus.
	client, err := api.NewClient(api.Config{Address: promURL})
	if err != nil {
		log.Fatalf("Errore client Prometheus: %v", err)
	}
	promAPI := v1.NewAPI(client)

	// 4. Creazione File CSV Locale (Temporaneo).
	outDir := "tmp/export"
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("Errore creazione directory %s: %v", outDir, err)
	}
	fileName := fmt.Sprintf("%s/%s_report.csv", outDir, expName)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Errore creazione file CSV: %v", err)
	}

	writer := csv.NewWriter(file)
	_ = writer.Write([]string{
		"Timestamp", "ID_Nodo", "Type", "MetricName", "Query", "Comment", "Valore", "Labels",
	})

	// 5. Estrazione Dati da Prometheus.
	ctx := context.Background()
	queryRange := v1.Range{Start: start, End: end, Step: step}

	fmt.Printf("📊 Avvio estrazione metriche per l'esperimento: %s...\n", expName)
	for _, m := range metrics {
		if m.Query == "" {
			fmt.Printf("⚠️ Salto entry %q: query vuota\n", m.MetricName)
			continue
		}

		result, _, err := promAPI.QueryRange(ctx, m.Query, queryRange)
		if err != nil {
			fmt.Printf("⚠️ Errore query %s (%s): %v\n", m.MetricName, m.Query, err)
			continue
		}

		matrix, ok := result.(model.Matrix)
		if !ok {
			fmt.Printf("⚠️ Query %s: risultato non Matrix (tipo=%T), salto\n", m.MetricName, result)
			continue
		}

		for _, series := range matrix {
			nodeID := string(series.Metric["nodo_id"])
			if nodeID == "" {
				nodeID = "unknown"
			}
			allLabels := series.Metric.String()

			for _, pair := range series.Values {
				_ = writer.Write([]string{
					pair.Timestamp.Time().Format(time.RFC3339),
					nodeID,
					m.Type,
					m.MetricName,
					m.Query,
					m.Comment,
					pair.Value.String(),
					allLabels,
				})
			}
		}
	}

	// Chiudiamo writer + file prima di leggerlo / caricarlo.
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatalf("Errore CSV writer: %v", err)
	}
	if err := file.Close(); err != nil {
		log.Fatalf("Errore chiusura file CSV: %v", err)
	}
	fmt.Printf("✅ Export locale completato: %s\n", fileName)

	// 6. Destinazione: Google Drive se configurato, altrimenti stdout.
	if gdriveFolderID == "" || gdriveCredPath == "" {
		fmt.Println("📋 Google Drive non configurato. Dump CSV su stdout:")
		dumpToStdout(fileName)
		return
	}

	if err := uploadToGoogleDrive(ctx, fileName, expName, gdriveFolderID, gdriveCredPath); err != nil {
		log.Fatalf("❌ Upload Google Drive: %v", err)
	}
}

// dumpToStdout legge il CSV e lo stampa su stdout, racchiuso tra marker per
// facilitare il recovery via `kubectl logs job/<exp>-exporter-job`.
func dumpToStdout(fileName string) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("read csv: %v", err)
	}
	fmt.Println("----- BEGIN CSV -----")
	fmt.Print(string(data))
	fmt.Println("----- END CSV -----")
}

// uploadToGoogleDrive carica il CSV nella cartella Drive identificata da
// folderID, autenticandosi con il service-account JSON letto da credPath.
func uploadToGoogleDrive(ctx context.Context, fileName, expName, folderID, credPath string) error {
	credJSON, err := os.ReadFile(credPath)
	if err != nil {
		return fmt.Errorf("read credentials: %w", err)
	}

	cfg, err := google.JWTConfigFromJSON(credJSON, drive.DriveFileScope)
	if err != nil {
		return fmt.Errorf("parse credentials: %w", err)
	}

	driveSvc, err := drive.NewService(ctx, option.WithHTTPClient(cfg.Client(ctx)))
	if err != nil {
		return fmt.Errorf("init drive service: %w", err)
	}

	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("open csv: %w", err)
	}
	defer f.Close()

	meta := &drive.File{
		Name:    fmt.Sprintf("%s_report.csv", expName),
		Parents: []string{folderID},
	}
	fmt.Printf("📤 Upload Google Drive (folder=%s)...\n", folderID)
	res, err := driveSvc.Files.Create(meta).
		SupportsAllDrives(true).
		Media(f).
		Do()
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	fmt.Printf("🚀 Caricato su Drive (id=%s, size=%d)\n", res.Id, res.Size)
	return nil
}
