package cachex

import (
	"os"
	"strconv"

	lru "github.com/hashicorp/golang-lru/v2"
)

// Cache adalah wrapper untuk LRU (Least Recently Used) cache.
// LRU cache digunakan untuk menyimpan hot keys di memory aplikasi (bukan di Redis).
// Ini mengurangi latency untuk data yang sangat sering diakses.
// Cache ini mengimplementasikan cache-aside pattern di level aplikasi.
type Cache struct {
	Enabled bool                      // Flag apakah cache enabled atau tidak
	LRU     *lru.Cache[string, string] // LRU cache instance
}

// NewLRU membuat instance Cache baru.
// Cache bisa di-enable/disable melalui environment variable LOCAL_CACHE_HOTKEYS.
// Ukuran cache bisa di-set melalui environment variable LOCAL_CACHE_SIZE (default: 1024 entries).
// Jika creation gagal, cache akan di-disable agar aplikasi tetap bisa berjalan.
func NewLRU() *Cache {
	// Check apakah cache enabled (default: enabled jika env var tidak di-set atau != "0")
	enabled := os.Getenv("LOCAL_CACHE_HOTKEYS") != "0"
	if !enabled {
		return &Cache{Enabled: false}
	}

	// Set default size: 1024 entries
	size := 1024
	// Override dengan environment variable jika ada
	if s := os.Getenv("LOCAL_CACHE_SIZE"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			size = v
		}
	}

	// Buat LRU cache dengan size yang ditentukan
	c, err := lru.New[string, string](size)
	if err != nil {
		// Jika creation gagal, fallback ke disabled cache
		// Ini memastikan aplikasi tetap bisa berjalan meskipun cache tidak tersedia
		return &Cache{Enabled: false}
	}
	return &Cache{Enabled: true, LRU: c}
}
