package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/gocql/gocql"
)

func main() {
	hostname := flag.String("h", "localhost", "Cassandra Hostname")
	keyspace := flag.String("k", "newts", "Newts Keyspace")
	pagesize := flag.Int("p", 1000, "page size")
	flag.Parse()

	cluster := gocql.NewCluster(*hostname)
	cluster.Keyspace = *keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatalf("Cannot connect to Cassandra: %v", err)
	}
	defer session.Close()

	log.Printf("Analyzing the %s.resource_metrics table...", *keyspace)
	var pageState []byte
	samples := 0
	resources := make(map[string]int)
	metrics := make(map[string]int)
	for {
		iter := session.Query(`select metric_name, resource from resource_metrics`).PageSize(*pagesize).PageState(pageState).Iter()
		nextPageState := iter.PageState()
		scanner := iter.Scanner()
		for scanner.Next() {
			var metric, resource string
			err = scanner.Scan(&metric, &resource)
			if err != nil {
				log.Fatal(err)
			}
			resources[resource]++
			metrics[metric]++
			samples++
		}
		err = scanner.Err()
		if err != nil {
			log.Fatal(err)
		}
		if len(nextPageState) == 0 {
			break
		}
		pageState = nextPageState
	}

	if f, err := os.Create("metrics.txt"); err == nil {
		for _, k := range getKeys(metrics) {
			f.WriteString(fmt.Sprintf("%s=%d\n", k, metrics[k]))
		}
		f.Sync()
		f.Close()
	}

	if f, err := os.Create("resources.txt"); err == nil {
		for _, k := range getKeys(resources) {
			f.WriteString(fmt.Sprintf("%s=%d\n", k, resources[k]))
		}
		f.Sync()
		f.Close()
	}

	log.Printf("Number of Active Samples: %v", samples)
	log.Printf("Number of Unique Metrics: %v", len(metrics))
	log.Printf("Number of Unique Resources: %v", len(resources))
}

func getKeys(data map[string]int) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
