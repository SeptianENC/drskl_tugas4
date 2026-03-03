package redisx

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// NewCluster membuat koneksi ke Redis cluster.
// Cluster address diambil dari environment variable REDIS_STARTUP_NODES
// yang berformat: "host1:port1,host2:port2,host3:port3"
// Client ini akan otomatis discover semua node di cluster setelah koneksi pertama.
func NewCluster() *redis.ClusterClient {
	nodes := os.Getenv("REDIS_STARTUP_NODES")
	if nodes == "" {
		nodes = "redis-1:7001"
	}
	addrs := strings.Split(nodes, ",")

	// Buat cluster client dengan konfigurasi timeout yang reasonable
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        addrs,                    // List node addresses untuk initial connection
		ReadTimeout:  2 * time.Second,          // Timeout untuk read operations
		WriteTimeout: 2 * time.Second,          // Timeout untuk write operations
		DialTimeout:  2 * time.Second,          // Timeout untuk establish connection
	})
}

// ClusterMemRatio menghitung rasio penggunaan memori di seluruh Redis cluster.
// Return value adalah float64 antara 0-1 yang menunjukkan:
// - 0.0 = tidak ada memori yang digunakan
// - 1.0 = memori penuh (100% dari maxmemory)
// Fungsi ini penting untuk menentukan kapan harus overflow data ke HDFS.
func ClusterMemRatio(ctx context.Context, c *redis.ClusterClient) (float64, error) {
	var usedTotal, maxTotal int64

	// Iterate melalui semua shard (node) di cluster
	// Setiap shard adalah master node yang menyimpan sebagian data
	err := c.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		// Ambil informasi memori dari setiap shard menggunakan INFO command
		s, err := shard.Info(ctx, "memory").Result()
		if err != nil {
			return nil // Tolerate error dari beberapa shard (partial failure OK)
		}
		// Parse used_memory dan maxmemory dari output INFO
		used := parseInfoInt(s, "used_memory")
		maxm := parseInfoInt(s, "maxmemory")
		// Akumulasi total used dan max memory dari semua shard
		usedTotal += used
		maxTotal += maxm
		return nil
	})

	// Jika maxTotal <= 0, berarti tidak ada maxmemory yang di-set atau error
	if maxTotal <= 0 {
		return 0, err
	}
	// Hitung rasio: used / max
	return float64(usedTotal) / float64(maxTotal), err
}

// parseInfoInt memparse nilai integer dari output Redis INFO command.
// Format INFO: "key:value\n" atau "key:value\r\n"
// Fungsi ini mencari baris yang dimulai dengan "key:" dan mengambil valuenya.
func parseInfoInt(info string, key string) int64 {
	lines := strings.Split(info, "\n")
	prefix := key + ":"
	for _, ln := range lines {
		if strings.HasPrefix(ln, prefix) {
			// Ekstrak value setelah prefix
			v := strings.TrimSpace(strings.TrimPrefix(ln, prefix))
			// Convert ke int64
			n, _ := toInt64(v)
			return n
		}
	}
	return 0
}

// toInt64 mengkonversi string ke int64.
// Fungsi ini hanya membaca digit numerik sampai menemukan karakter non-digit.
// Contoh: "12345" -> 12345, "12345bytes" -> 12345
func toInt64(s string) (int64, error) {
	var n int64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			break // Stop jika menemukan karakter non-digit
		}
		n = n*10 + int64(s[i]-'0')
	}
	return n, nil
}
