package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type TableSchema struct {
	TableName string   `json:"table_name"`
	Columns   []Column `json:"columns"`
	Indexes   []Index  `json:"indexes"`
}

type Column struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	IsNullable   bool   `json:"is_nullable"`
	IsPrimaryKey bool   `json:"is_primary_key"`
	DefaultValue string `json:"default_value"`
	Extra        string `json:"extra"`
	CharacterSet string `json:"character_set"`
	Collation    string `json:"collation"`
}

type Index struct {
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	IsUnique  bool     `json:"is_unique"`
	IndexType string   `json:"index_type"`
}

// Connect establishes a connection to SingleStore
func Connect() (*sql.DB, error) {
	err := godotenv.Load()
	if err != nil {
		return nil, fmt.Errorf("error loading .env file: %v", err)
	}

	url := os.Getenv("SINGLESTORE_URL")
	if url == "" {
		return nil, fmt.Errorf("SINGLESTORE_URL not found in environment")
	}

	// Format: admin:password@hostname:port/
	// Convert the hostname to TCP protocol format and add database name
	url = strings.Replace(url, "@", "@tcp(", 1)
	if strings.Contains(url, ":3306/") {
		url = strings.Replace(url, ":3306/", ")/db", 1)
	} else {
		url = strings.Replace(url, "/", ")/db", 1)
	}

	// Add parameters if not present
	if !strings.Contains(url, "?") {
		url += "?parseTime=true&tls=true"
	}

	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, fmt.Errorf("error connecting to SingleStore: %v", err)
	}

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("error pinging SingleStore: %v", err)
	}

	// Initialize stored procedures
	if err = initStoredProcedures(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("error initializing stored procedures: %v", err)
	}

	return db, nil
}

// initStoredProcedures creates the stored procedures for batch inserts
func initStoredProcedures(db *sql.DB) error {
	procedures := []string{
		`CREATE OR REPLACE PROCEDURE InsertBlockRewards(rewards ARRAY(RECORD(
			slot BIGINT, reward_index INT, pubkey TEXT, lamports BIGINT, post_balance BIGINT,
			reward_type TEXT, commission BIGINT, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("block_rewards", rewards);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactions(transactions ARRAY(RECORD(
			slot BIGINT, transaction_index INT, block_time TIMESTAMP, block_hash TEXT, fee BIGINT,
			compute_units_consumed BIGINT, compute_units_price BIGINT, err TEXT, err_message TEXT,
			err_stack TEXT, err_instruction_index BIGINT, err_custom_code BIGINT, err_custom_message TEXT,
			successful BOOLEAN, version TEXT, recent_blockhash TEXT,
			num_readonly_signed_accounts BIGINT, num_readonly_unsigned_accounts BIGINT,
			num_required_signatures BIGINT, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transactions", transactions);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactionSignatures(signatures ARRAY(RECORD(
			signature TEXT, slot BIGINT, transaction_index INT, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transactions_signatures", signatures);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactionInstructions(instructions ARRAY(RECORD(
			slot BIGINT, transaction_index INT, instruction_index INT, program_id TEXT,
			program_id_index INT, stack_height BIGINT, data TEXT, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transaction_instructions", instructions);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactionInnerInstructions(innerInstructions ARRAY(RECORD(
			slot BIGINT, slot_index INT, instruction_index INT, inner_instruction_index INT,
			program_id TEXT, program_id_index INT, data TEXT, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transaction_inner_instructions", innerInstructions);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactionLogs(logs ARRAY(RECORD(
			slot BIGINT, transaction_index INT, log_index INT, log TEXT, level TEXT,
			program_id TEXT, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transaction_logs", logs);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactionAccounts(accounts ARRAY(RECORD(
			slot BIGINT, transaction_index INT, account_index INT, pubkey TEXT,
			account_address TEXT, is_signer BOOLEAN, is_writable BOOLEAN, pre_balance BIGINT,
			post_balance BIGINT, balance_change BIGINT, source TEXT, rent_epoch_change BIGINT,
			updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transaction_accounts", accounts);
		END;`,

		`CREATE OR REPLACE PROCEDURE InsertTransactionTokenBalances(balances ARRAY(RECORD(
			slot BIGINT, transaction_index INT, account_index INT, mint TEXT, owner TEXT,
			ui_token_amount DOUBLE, amount TEXT, decimals INT, pre_amount TEXT, pre_ui_amount DOUBLE,
			post_amount TEXT, post_ui_amount DOUBLE, updated_at TIMESTAMP, created_at TIMESTAMP
		))) AS
		DECLARE
			x INT;
		BEGIN
			x = INSERT_ALL("transaction_token_balances", balances);
		END;`,
	}

	for _, proc := range procedures {
		if _, err := db.Exec(proc); err != nil {
			return fmt.Errorf("error creating stored procedure: %v", err)
		}
	}
	return nil
}

// IntrospectSchema gets the schema information for all tables
func IntrospectSchema(db *sql.DB) ([]TableSchema, error) {
	// Get current database name
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("error getting current database: %v", err)
	}

	// Get all tables
	rows, err := db.Query(`
		SELECT TABLE_NAME 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ?`, dbName)
	if err != nil {
		return nil, fmt.Errorf("error querying tables: %v", err)
	}
	defer rows.Close()

	var schemas []TableSchema
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("error scanning table name: %v", err)
		}

		schema, err := getTableSchema(db, dbName, tableName)
		if err != nil {
			return nil, fmt.Errorf("error getting schema for table %s: %v", tableName, err)
		}
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// getTableSchema gets the detailed schema for a single table
func getTableSchema(db *sql.DB, dbName, tableName string) (TableSchema, error) {
	schema := TableSchema{
		TableName: tableName,
	}

	// Get columns
	columns, err := db.Query(`
		SELECT 
			COLUMN_NAME,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_KEY,
			COLUMN_DEFAULT,
			EXTRA,
			CHARACTER_SET_NAME,
			COLLATION_NAME
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`,
		dbName, tableName)
	if err != nil {
		return schema, err
	}
	defer columns.Close()

	for columns.Next() {
		var col Column
		var isNullable, columnKey, defaultValue, charSet, collation sql.NullString
		if err := columns.Scan(
			&col.Name,
			&col.Type,
			&isNullable,
			&columnKey,
			&defaultValue,
			&col.Extra,
			&charSet,
			&collation,
		); err != nil {
			return schema, err
		}

		col.IsNullable = isNullable.String == "YES"
		col.IsPrimaryKey = columnKey.String == "PRI"
		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}
		if charSet.Valid {
			col.CharacterSet = charSet.String
		}
		if collation.Valid {
			col.Collation = collation.String
		}

		schema.Columns = append(schema.Columns, col)
	}

	// Get indexes
	indexes, err := db.Query(`
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			NON_UNIQUE,
			INDEX_TYPE
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
		dbName, tableName)
	if err != nil {
		return schema, err
	}
	defer indexes.Close()

	indexMap := make(map[string]*Index)
	for indexes.Next() {
		var indexName, columnName, indexType string
		var nonUnique bool
		if err := indexes.Scan(&indexName, &columnName, &nonUnique, &indexType); err != nil {
			return schema, err
		}

		if idx, exists := indexMap[indexName]; exists {
			idx.Columns = append(idx.Columns, columnName)
		} else {
			indexMap[indexName] = &Index{
				Name:      indexName,
				Columns:   []string{columnName},
				IsUnique:  !nonUnique,
				IndexType: indexType,
			}
		}
	}

	for _, idx := range indexMap {
		schema.Indexes = append(schema.Indexes, *idx)
	}

	return schema, nil
}

// SaveSchemaToFile saves the schema information to a JSON file
func SaveSchemaToFile(schemas []TableSchema, filename string) error {
	data, err := json.MarshalIndent(schemas, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling schema to JSON: %v", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("error writing schema to file: %v", err)
	}

	return nil
}

// IntrospectAndSaveSchema connects to the database, introspects the schema, and saves it to a file
func IntrospectAndSaveSchema(filename string) error {
	db, err := Connect()
	if err != nil {
		return fmt.Errorf("error connecting to database: %v", err)
	}
	defer db.Close()

	schemas, err := IntrospectSchema(db)
	if err != nil {
		return fmt.Errorf("error introspecting schema: %v", err)
	}

	if err := SaveSchemaToFile(schemas, filename); err != nil {
		return fmt.Errorf("error saving schema to file: %v", err)
	}

	return nil
}
