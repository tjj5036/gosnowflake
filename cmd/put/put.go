package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strconv"

	sf "github.com/snowflakedb/gosnowflake"
	"os"
)

// getDSN constructs a DSN based on the test connection parameters
func getDSN() (string, *sf.Config, error) {

	env := func(k string) string {
		if value := os.Getenv(k); value != "" {
			return value
		}
		log.Fatalf("%v environment variable is not set.", k)
		return ""
	}

	account := env("SNOWFLAKE_TEST_ACCOUNT")
	user := env("SNOWFLAKE_TEST_USER")
	password := env("SNOWFLAKE_TEST_PASSWORD")
	host := env("SNOWFLAKE_TEST_HOST")
	port := env("SNOWFLAKE_TEST_PORT")
	protocol := env("SNOWFLAKE_TEST_PROTOCOL")
	database := env("SNOWFLAKE_TEST_DATABASE")
	schema := env("SNOWFLAKE_TEST_SCHEMA")

	portStr, _ := strconv.Atoi(port)
	cfg := &sf.Config{
		Account:  account,
		User:     user,
		Password: password,
		Host:     host,
		Port:     portStr,
		Protocol: protocol,
		Database: database,
		Schema:   schema,
	}

	dsn, err := sf.DSN(cfg)
	return dsn, cfg, err
}

func main() {
	if !flag.Parsed() {
		// enable glog for Go Snowflake Driver
		flag.Parse()
	}

	dsn, cfg, err := getDSN()
	if err != nil {
		log.Fatalf("failed to create DSN from Config: %v, err: %v", cfg, err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect. %v, err: %v", dsn, err)
	}
	defer db.Close()

	// mystage created via `create stage mystage url='file:///tmp';`
	query := "put file://~/to_load/* @mystage auto_compress=true"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalf("failed to run a query. %v, err: %v", query, err)
	}
	defer rows.Close()
	for rows.Next() {
		var src string
		var dst string
		var srcSize int64
		var dstSize int64
		var srcCompression sql.NullString
		var dstCompression sql.NullString
		var status string
		var message sql.NullString

		err := rows.Scan(&src, &dst, &srcSize, &dstSize, &srcCompression, &dstCompression, &status, &message)
		if err != nil {
			log.Fatalf("failed to get result. err: %v", err)
		}

		fmt.Printf("source: %s (size %d, compression %s) target: %s (size %d compression %s) status %s msg %s\n",
			src, srcSize, srcCompression.String, dst, dstSize, dstCompression.String, status, message.String)
	}
	if rows.Err() != nil {
		fmt.Printf("ERROR: %v\n", rows.Err())
		return
	}
}
