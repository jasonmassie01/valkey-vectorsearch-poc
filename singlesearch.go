// search.go - Script to perform a search with a given term (passed as command-line argument) and return results ranked by ad quality score

package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type AdResult struct {
	Key   string
	Term  string
	Score float64
}

func generateVector(term string) []byte {
	h := fnv.New64a()
	h.Write([]byte(term))
	seed := int64(h.Sum64())

	r := rand.New(rand.NewSource(seed))
	vector := make([]float32, 128)
	for j := range vector {
		vector[j] = r.Float32() * 2 - 1
	}
	vecBytes := make([]byte, 128*4)
	for j, v := range vector {
		binary.LittleEndian.PutUint32(vecBytes[j*4:], math.Float32bits(v))
	}
	return vecBytes
}

func main() {
	// Parse command-line flag for search term
	searchTerm := flag.String("term", "", "The search term to query (required)")
	flag.Parse()

	if *searchTerm == "" {
		fmt.Println("Error: Search term is required. Usage: go run search.go -term=\"buy iphone\"")
		os.Exit(1)
	}

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

	// Generate query vector based on search term (consistent for same term)
	vecBytes := generateVector(*searchTerm)

	// Construct FT.SEARCH command
	args := []interface{}{
		"FT.SEARCH", "ad_index", "*=>[KNN 100 @vector $vec AS sim_score]",
		"PARAMS", "2", "vec", vecBytes,
		"DIALECT", "2",
		"RETURN", "5", "expected_ctr", "ad_relevance", "landing_experience", "sim_score", "search_term",
	}
	searchCmd := redis.NewSliceCmd(ctx, args...)
	if err := client.Process(ctx, searchCmd); err != nil {
		fmt.Println("Error executing FT.SEARCH:", err)
		return
	}

	// Parse search results
	res := searchCmd.Val()
	if len(res) == 0 {
		fmt.Println("No results found")
		return
	}
	total := res[0].(int64)
	var ads []AdResult
	for i := 1; i < len(res); i += 2 {
		key := res[i].(string)
		fields := res[i+1].([]interface{})
		if len(fields) < 10 { // 5 fields * 2 (name + value)
			continue // Skip malformed
		}
		ctr, _ := fields[1].(string)
		rel, _ := fields[3].(string)
		land, _ := fields[5].(string)
		sim, _ := fields[7].(string)
		term, _ := fields[9].(string)

		ctrVal := parseFloat(ctr)
		relVal := parseFloat(rel)
		landVal := parseFloat(land)
		simVal := parseFloat(sim)

		// Calculate points (adjust thresholds as needed)
		ctrPoints := 0.0
		if ctrVal > 0.05 {
			ctrPoints = 3.5
		} else if ctrVal > 0.02 {
			ctrPoints = 1.75
		}
		relPoints := 0.0
		if relVal > 0.8 {
			relPoints = 2
		} else if relVal > 0.5 {
			relPoints = 1
		}
		landPoints := 0.0
		if landVal > 0.7 {
			landPoints = 3.5
		} else if landVal > 0.4 {
			landPoints = 1.75
		}

		qualityScore := 1 + ctrPoints + relPoints + landPoints

		// Optionally weight by similarity: since sim is distance, lower better for COSINE
		qualityScore *= (1 - simVal)

		ads = append(ads, AdResult{Key: key, Score: qualityScore, Term: term})
	}

	// Sort ads by quality_score descending
	sort.Slice(ads, func(i, j int) bool {
		return ads[i].Score > ads[j].Score
	})

	// Print ranked ads
	fmt.Printf("Found %d results for '%s', ranked by quality score:\n", total, *searchTerm)
	for _, ad := range ads {
		fmt.Printf("%s (Term: %s): %.1f\n", ad.Key, ad.Term, ad.Score)
	}
}

func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
}
