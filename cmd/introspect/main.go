package main

import (
	"fmt"
	"goblockstore/db"
	"log"
)

func main() {
	err := db.IntrospectAndSaveSchema("schema.json")
	if err != nil {
		log.Fatalf("Error introspecting schema: %v", err)
	}
	fmt.Println("Schema saved to schema.json")
}
