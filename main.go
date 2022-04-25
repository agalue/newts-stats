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
	hostname := flag.String("h", "localhost", "Cassandra hostname")
	keyspace := flag.String("k", "newts", "Newts Keyspace")
	port := flag.Int("p", 9042, "Cassandra port")
	pagesize := flag.Int("n", 1000, "page size")
	username := flag.String("user", "", "Cassandra username")
	password := flag.String("pwd", "", "Cassandra password")
	certpath := flag.String("cacert", "", "Path to Server CA certificate")
	outputdir := flag.String("o", "/tmp", "Output directory")
	flag.Parse()

	cluster := gocql.NewCluster(fmt.Sprintf("%s:%d", *hostname, *port))
	cluster.Keyspace = *keyspace
	cluster.Consistency = gocql.LocalQuorum

	if *username != "" && *password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *username,
			Password: *password,
		}
	}

	if *certpath != "" {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath: *certpath,
		}
	}

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

	log.Printf("Number of Active Samples: %v", samples)
	log.Printf("Number of Unique Metrics: %v", len(metrics))
	log.Printf("Number of Unique Resources: %v", len(resources))

	metricsFile := *outputdir + "/metrics.txt"
	if f, err := os.Create(metricsFile); err == nil {
		for _, k := range getKeys(metrics) {
			f.WriteString(fmt.Sprintf("%s=%d\n", k, metrics[k]))
		}
		f.Sync()
		f.Close()
		log.Printf("Metrics details written to %s", metricsFile)
	} else {
		log.Fatalf("Cannot write file %s: %v", metricsFile, err)
	}

	resourcesFile := *outputdir + "/resources.txt"
	if f, err := os.Create(resourcesFile); err == nil {
		for _, k := range getKeys(resources) {
			f.WriteString(fmt.Sprintf("%s=%d\n", k, resources[k]))
		}
		f.Sync()
		f.Close()
		log.Printf("Resources details written to %s", resourcesFile)
	} else {
		log.Fatalf("Cannot write file %s: %v", resourcesFile, err)
	}
}

func getKeys(data map[string]int) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
