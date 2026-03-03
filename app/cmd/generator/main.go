package main

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// Event merepresentasikan data event yang akan dikirim ke ingestor service
type Event struct {
	Key        string         `json:"key"`
	Value      map[string]any `json:"value"`
	TTLSeconds int            `json:"ttl_sec"`
	CacheHint  string         `json:"cache_hint"`
}

func main() {
	// Ambil URL ingestor service dari environment variable
	ingestorURL := os.Getenv("INGESTOR_URL")
	if ingestorURL == "" {
		ingestorURL = "http://localhost:8080"
	}
	// RPS (Requests Per Second): jumlah request yang dikirim per detik
	rps := getInt("RPS", 200)
	// HotRatio: proporsi request yang menggunakan hot keys (keys yang sering diakses)
	hotRatio := getFloat("HOTKEY_RATIO", 0.2)

	// Generate 50 hot keys yang akan digunakan berulang-ulang
	// Hot keys ini mensimulasikan data yang sering diakses (seperti trending videos)
	hotKeys := make([]string, 50)
	for i := range hotKeys {
		hotKeys[i] = "feature:HOT:" + strconv.Itoa(i)
	}

	// Seed random number generator dengan waktu saat ini
	rand.Seed(time.Now().UnixNano())
	// Hitung interval antara setiap request berdasarkan RPS
	interval := time.Second / time.Duration(max(1, rps))

	// Loop utama: generate dan kirim event secara kontinyu
	for {
		// Pilih key secara random: hot key atau cold key berdasarkan hotRatio
		key := pickKey(hotKeys, hotRatio)
		// Buat event dengan data simulasi (user action, video interaction, dll)
		ev := Event{
			Key: key,
			Value: map[string]any{
				"user_id":    rand.Intn(1_000_000),                    // Random user ID
				"video_id":   rand.Intn(5_000_000),                   // Random video ID
				"ts":         float64(time.Now().UnixNano()) / 1e9,   // Timestamp dalam detik
				"watch_time": rand.Float64() * 30.0,                  // Waktu menonton (0-30 detik)
			},
			TTLSeconds: 3600, // TTL 1 jam
			CacheHint:  "none",
		}
		// Jika key adalah hot key, tandai dengan cache hint "hot_read"
		// Ini memberi sinyal ke ingestor untuk cache data ini di local LRU
		if len(key) >= 12 && key[:12] == "feature:HOT" {
			ev.CacheHint = "hot_read"
		}

		// Serialize event ke JSON dan kirim ke ingestor service
		b, _ := json.Marshal(ev)
		_, _ = http.Post(ingestorURL+"/ingest", "application/json", bytes.NewReader(b))
		// Tunggu interval sebelum kirim request berikutnya
		time.Sleep(interval)
	}
}

// pickKey memilih key secara random berdasarkan hotRatio.
// Jika random number < hotRatio, pilih hot key; jika tidak, generate cold key baru.
// Ini mensimulasikan distribusi traffic: sebagian besar cold, sebagian kecil hot.
func pickKey(hotKeys []string, hotRatio float64) string {
	if rand.Float64() < hotRatio {
		return hotKeys[rand.Intn(len(hotKeys))]
	}
	// Generate cold key dengan UUID untuk memastikan unik
	return "feature:COLD:" + uuid.NewString()
}

// getInt membaca integer dari environment variable dengan default value
func getInt(env string, def int) int {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}
	return def
}

// getFloat membaca float dari environment variable dengan default value
func getFloat(env string, def float64) float64 {
	if s := os.Getenv(env); s != "" {
		if v, err := strconv.ParseFloat(s, 64); err == nil {
			return v
		}
	}
	return def
}

// max mengembalikan nilai maksimum antara dua integer
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
