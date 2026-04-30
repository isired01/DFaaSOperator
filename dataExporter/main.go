package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	// 1. Recupero parametri dalle Env Var (Configurate dall'Operatore)
	promURL := os.Getenv("PROM_URL")
	queries := strings.Split(os.Getenv("QUERIES"), "|")
	startStr := os.Getenv("START_TIME")
	endStr := os.Getenv("END_TIME")
	stepStr := os.Getenv("STEP")

	expName := os.Getenv("EXP_NAME")

	// Credenziali MinIO (Assicurati che l'operatore le passi correttamente)
	minioEndpoint := os.Getenv("MINIO_ENDPOINT") // Es: minio-service.monitoring:9000
	minioAccessKey := os.Getenv("MINIO_ACCESS_KEY")
	minioSecretKey := os.Getenv("MINIO_SECRET_KEY")
	bucketName := "dfaas-results"

	// 2. Parsing dei tempi e dello step
	start, _ := time.Parse(time.RFC3339, startStr)
	end, _ := time.Parse(time.RFC3339, endStr)
	step, _ := time.ParseDuration(stepStr)

	// 3. Setup Client Prometheus
	client, err := api.NewClient(api.Config{Address: promURL})
	if err != nil {
		log.Fatalf("Errore client Prometheus: %v", err)
	}
	promAPI := v1.NewAPI(client)

	outDir := "tmp/export"
	// 4. Creazione File CSV Locale (Temporaneo)
	_ = os.MkdirAll(outDir, 0755)
	fileName := fmt.Sprintf("%s/%s_report.csv", outDir, expName)
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Errore creazione file CSV: %v", err)
	}

	writer := csv.NewWriter(file)
	writer.Write([]string{"Timestamp", "ID_Nodo", "Query", "Valore", "Labels"})

	// 5. Estrazione Dati da Prometheus
	ctx := context.Background()
	queryRange := v1.Range{Start: start, End: end, Step: step}

	fmt.Printf("📊 Avvio estrazione metriche per l'esperimento: %s...\n", expName)
	for _, q := range queries {
		if q == "" {
			continue
		}

		result, _, err := promAPI.QueryRange(ctx, q, queryRange)
		if err != nil {
			fmt.Printf("⚠️ Errore query %s: %v\n", q, err)
			continue
		}

		matrix, _ := result.(model.Matrix)
		for _, series := range matrix {
			nodeID := string(series.Metric["nodo_id"])
			if nodeID == "" {
				nodeID = "unknown"
			}
			allLabels := series.Metric.String()

			for _, pair := range series.Values {
				writer.Write([]string{
					pair.Timestamp.Time().Format(time.RFC3339),
					nodeID,
					q,
					pair.Value.String(),
					allLabels,
				})
			}
		}
	}

	// Fondamentale: chiudiamo il writer e il file prima di caricarlo su MinIO
	writer.Flush()
	file.Close()
	fmt.Printf("✅ Export locale completato: %s\n", fileName)

	// 6. Upload su MinIO (Object Storage)
	// -----------------------------------------------------------------

	// Inizializzazione Client MinIO
	minioClient, err := minio.New(minioEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
		Secure: false, // Usiamo HTTP interno al cluster
	})
	if err != nil {
		log.Fatalf("❌ Errore setup client MinIO: %v", err)
	}

	// Creazione bucket se non esiste (lo facciamo qui così non devi farlo a mano)
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err == nil && !exists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		fmt.Printf("📦 Bucket '%s' creato con successo\n", bucketName)
	}
	if err != nil && !exists {
		log.Fatalf("❌ Errore verifica/creazione bucket MinIO: %v", err)
	}

	// Carichiamo il file in una "cartella" virtuale col nome dell'esperimento
	objectName := fmt.Sprintf("%s/%s_report.csv", expName, expName)

	fmt.Printf("📤 Caricamento in corso su MinIO (%s)... ", objectName)
	info, err := minioClient.FPutObject(ctx, bucketName, objectName, fileName, minio.PutObjectOptions{
		ContentType: "text/csv",
	})
	if err != nil {
		log.Fatalf("❌ Errore upload: %v", err)
	}

	fmt.Printf("🚀 Completato! (%d byte salvati)\n", info.Size)
}
