package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand"
	"os"
	"time"
	"sync"
	"github.com/jamiealquiza/tachymeter"
	"github.com/redis/go-redis/v9"
)

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

func performSearch(client *redis.ClusterClient, searchTerm string) (int64, float64, []interface{}) {
	start := time.Now()
	ctx := context.Background()
	vecBytes := generateVector(searchTerm)
	args := []interface{}{
		"FT.SEARCH", "ad_index", "*=>[KNN 100 @vector $vec AS sim_score]",
		"PARAMS", "2", "vec", vecBytes,
		"DIALECT", "2",
		"RETURN", "5", "expected_ctr", "ad_relevance", "landing_experience", "sim_score", "search_term",
	}
	searchCmd := redis.NewSliceCmd(ctx, args...)
	if err := client.Process(ctx, searchCmd); err != nil {
		fmt.Println("Error executing FT.SEARCH:", err)
		return 0, 0, nil
	}
	res := searchCmd.Val()
	total := int64(0)
	if len(res) > 0 {
		total = res[0].(int64)
	}
	latency := time.Since(start).Seconds() * 1000 // ms
	return total, latency, res
}

func main() {
	// Load search terms from file
	file, err := os.Open("search_terms.txt")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()
	var terms []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		terms = append(terms, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	if len(terms) == 0 {
		fmt.Println("No terms found in file")
		return
	}

	// Connect to Valkey with connection pool settings
	addr := "REPLACE:6379"
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          []string{addr},
		PoolSize:       1000,              // Max number of connections in the pool
		MinIdleConns:   100,               // Minimum number of idle connections
		ConnMaxLifetime: 30 * time.Minute, // Close connections after 30 minutes
		PoolTimeout:    5 * time.Second,  // Timeout for getting a connection from the pool
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		fmt.Println("Error pinging cluster:", err)
		return
	}
	defer client.Close()

	// Initialize tachymeter
	tach := tachymeter.New(&tachymeter.Config{Size: 10000})

	// Concurrency level
	concurrency := 1000
	numSearchesPerBatch := 1 // Searches per batch

	// Loop for 5 minutes
	startTime := time.Now()
	endTime := startTime.Add(5 * time.Minute)
	sampleDone := false // Flag to print results only once

	for time.Now().Before(endTime) {
		var wg sync.WaitGroup
		latencies := make(chan float64, numSearchesPerBatch)
		resultsChan := make(chan []interface{}, 1) // For sample results
		for i := 0; i < numSearchesPerBatch; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				term := terms[rand.Intn(len(terms))]
				total, latency, res := performSearch(client, term)
				latencies <- latency
				_ = total // Use total to avoid unused variable error
				if !sampleDone && idx == 0 { // Capture results from first search in batch
					resultsChan <- res
				}
			}(i)
			if (i+1)%concurrency == 0 {
				time.Sleep(10 * time.Millisecond) // Reduced throttle
			}
		}
		wg.Wait()
		close(latencies)
		close(resultsChan)

		// Collect latencies from this batch
		for lat := range latencies {
			tach.AddTime(time.Duration(lat * float64(time.Millisecond)))
		}

		// Print sample results once for debugging
		if !sampleDone {
			if res, ok := <-resultsChan; ok && len(res) > 0 {
				fmt.Println("Sample Search Results for Verification (First Batch, First Search):")
				total := res[0].(int64)
				fmt.Printf("Total Results: %v\n", total)
				for j := 1; j < len(res); j += 2 {
					key := res[j].(string)
					fields := res[j+1].([]interface{})
					fmt.Printf("Key: %s, Fields: %v\n", key, fields)
				}
				sampleDone = true // Only print once
			}
		}
	}

	// Calculate and print stats using tachymeter
	results := tach.Calc()
	fmt.Println("------------------ Latency ------------------")
	fmt.Printf(
		"Count:\t\t%d\nMax:\t\t%s\nMin:\t\t%s\nP95:\t\t%s\nP99:\t\t%s\nP99.9:\t\t%s\n",
		results.Count,
		results.Time.Max,
		results.Time.Min,
		results.Time.P95,
		results.Time.P99,
		results.Time.P999,
	)
}
