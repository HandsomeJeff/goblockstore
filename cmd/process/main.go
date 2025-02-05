package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"goblockstore/parser"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Read block from stdin
	var block map[string]interface{}
	if err := json.NewDecoder(os.Stdin).Decode(&block); err != nil {
		log.Fatalf("Error decoding block: %v", err)
	}

	// Parse the block
	parsedBlock, err := parser.ParseBlock(block)
	if err != nil {
		log.Fatalf("Error parsing block: %v", err)
	}

	// Filter out vote transactions
	var filteredTxs []parser.Transaction
	for _, tx := range parsedBlock.Transactions {
		isVote := false
		for _, inst := range tx.Instructions {
			if inst.IsVote {
				isVote = true
				break
			}
		}
		if !isVote {
			filteredTxs = append(filteredTxs, tx)
		}
	}
	parsedBlock.Transactions = filteredTxs

	// Connect to SingleStore
	dbURL := os.Getenv("SINGLESTORE_URL")
	if dbURL == "" {
		log.Fatal("SINGLESTORE_URL environment variable not set")
	}

	// Format URL for SingleStore
	if dbURL[0:8] != "mysql://" {
		log.Fatal("SINGLESTORE_URL must start with mysql://")
	}
	dbURL = dbURL[8:] // Remove mysql:// prefix

	// Connect to database
	db, err := sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatalf("Error connecting to database: %v", err)
	}
	defer db.Close()

	// Save block to database
	if err := parser.SaveToDatabase(db, parsedBlock); err != nil {
		log.Fatalf("Error saving block to database: %v", err)
	}

	fmt.Printf("Successfully processed block %d\n", parsedBlock.Block.Slot)
}
