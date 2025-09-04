// load.go - Script to load 1000 sample records with ad quality metrics, random search term, and vector embedding

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

func randomTerm() string {
	words := []string{"cat", "dog", "car", "house", "computer", "phone", "book", "food", "travel", "music", "game", "movie", "sport", "tech", "health"}
	n := rand.Intn(3) + 1 // 1 to 3 words
	term := ""
	for i := 0; i < n; i++ {
		term += words[rand.Intn(len(words))] + " "
	}
	return term[:len(term)-1] // Trim trailing space
}

func main() {
	ctx := context.Background()
	// Use your Memorystore discovery endpoint and port (no TLS)
	addr := "REPLACE:6379"
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{addr},
		// No TLSConfig needed; add Username/Password if auth is enabled later
	})

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Println("Error pinging cluster:", err)
		return
	}
	defer client.Close()

	// Drop index if exists
	dropCmd := redis.NewStatusCmd(ctx, "FT.DROPINDEX", "ad_index")
	if err := client.Process(ctx, dropCmd); err != nil {
		fmt.Println("Warning: Error dropping index:", err)
	}

	// Create index on HASH (removed TEXT field as it's not supported in Valkey-Search yet; search_term can still be stored and returned but not indexed)
	schema := []interface{}{
		"FT.CREATE", "ad_index", "ON", "HASH", "PREFIX", "1", "ad:",
		"SCHEMA",
		"vector", "VECTOR", "HNSW", "6",
		"TYPE", "FLOAT32", "DIM", "128", "DISTANCE_METRIC", "COSINE",
		"expected_ctr", "NUMERIC",
		"ad_relevance", "NUMERIC",
		"landing_experience", "NUMERIC",
	}
	createCmd := redis.NewStatusCmd(ctx, schema...)
	if err := client.Process(ctx, createCmd); err != nil {
		fmt.Println("Error creating index:", err)
		return
	}

	// Seed random
	rand.Seed(time.Now().UnixNano())

	// Load 1000 sample ads
	for i := 0; i < 3000000; i++ {
		searchTerm := randomTerm()
		vector := make([]float32, 128)
		for j := range vector {
			vector[j] = rand.Float32() * 2 - 1 // Random between -1 and 1 as "embedding"
		}
		vecBytes := make([]byte, 128*4)
		for j, v := range vector {
			binary.LittleEndian.PutUint32(vecBytes[j*4:], math.Float32bits(v))
		}

		expectedCtr := rand.Float64() * 0.1 // 0 to 0.1
		adRelevance := rand.Float64()       // 0 to 1
		landingExperience := rand.Float64() // 0 to 1

		key := fmt.Sprintf("ad:%d", i)
		if err := client.HSet(ctx, key,
			"search_term", searchTerm,
			"vector", vecBytes,
			"expected_ctr", expectedCtr,
			"ad_relevance", adRelevance,
			"landing_experience", landingExperience,
		).Err(); err != nil {
			fmt.Println("Error setting hash:", err)
			return
		}
	}
	fmt.Println("Loaded 1000 sample ads.")
}
