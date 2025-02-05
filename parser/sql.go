package parser

import (
	"database/sql"
	"fmt"
	"strings"
)

// generateBatchInsertSQL generates an SQL statement for batch inserting multiple rows
func generateBatchInsertSQL(table string, columns []string, numRows int) string {
	// Create the base INSERT statement
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES ",
		table,
		strings.Join(columns, ", "))

	// Create the placeholders for each row
	placeholders := make([]string, numRows)
	singleRowPlaceholders := fmt.Sprintf("(%s)", strings.Repeat("?,", len(columns)-1)+"?")
	for i := range placeholders {
		placeholders[i] = singleRowPlaceholders
	}

	// Join all the placeholders with commas
	return sql + strings.Join(placeholders, ",")
}

// SaveToDatabase saves a parsed block to the database
func SaveToDatabase(db *sql.DB, block *ParsedBlock) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}
	defer tx.Rollback()

	// Save block
	_, err = tx.Exec(`
		INSERT INTO blocks (
			slot, parent_slot, block_time, block_height, blockhash, 
			previous_blockhash, transaction_count, successful, 
			updated_at, created_at
		) VALUES (?, ?, FROM_UNIXTIME(?), ?, ?, ?, ?, ?, ?, ?)`,
		block.Block.Slot,
		block.Block.ParentSlot,
		block.Block.BlockTime,
		block.Block.BlockHeight,
		block.Block.Blockhash,
		block.Block.PreviousBlockhash,
		block.Block.TransactionCount,
		block.Block.Successful,
		block.Block.UpdatedAt,
		block.Block.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("error inserting block: %v", err)
	}

	// Save block rewards in batches
	// if len(block.BlockRewards) > 0 {
	// 	columns := []string{"slot", "reward_index", "pubkey", "lamports", "post_balance",
	// 		"reward_type", "commission", "updated_at", "created_at"}
	// 	sql := generateBatchInsertSQL("block_rewards", columns, len(block.BlockRewards))

	// 	values := make([]interface{}, 0, len(block.BlockRewards)*len(columns))
	// 	for _, reward := range block.BlockRewards {
	// 		values = append(values,
	// 			reward.Slot,
	// 			reward.RewardIndex,
	// 			reward.Pubkey,
	// 			reward.Lamports,
	// 			reward.PostBalance,
	// 			reward.RewardType,
	// 			reward.Commission,
	// 			reward.UpdatedAt,
	// 			reward.CreatedAt,
	// 		)
	// 	}

	// 	_, err = tx.Exec(sql, values...)
	// 	if err != nil {
	// 		return fmt.Errorf("error batch inserting block rewards: %v", err)
	// 	}
	// }

	// Save transactions in batches
	if len(block.Transactions) > 0 {
		columns := []string{
			"slot", "transaction_index", "block_time", "block_hash", "fee",
			"compute_units_consumed", "compute_units_price", "err", "err_message",
			"err_stack", "err_instruction_index", "err_custom_code", "err_custom_message",
			"successful", "version", "recent_blockhash", "num_readonly_signed_accounts",
			"num_readonly_unsigned_accounts", "num_required_signatures", "updated_at", "created_at",
		}
		sql := generateBatchInsertSQL("transactions", columns, len(block.Transactions))

		values := make([]interface{}, 0, len(block.Transactions)*len(columns))
		for _, tx := range block.Transactions {
			values = append(values,
				tx.Slot,
				tx.TransactionIndex,
				tx.BlockTime,
				tx.BlockHash,
				tx.Fee,
				tx.ComputeUnitsConsumed,
				tx.ComputeUnitsPrice,
				tx.Err,
				tx.ErrMessage,
				tx.ErrStack,
				tx.ErrInstructionIndex,
				tx.ErrCustomCode,
				tx.ErrCustomMessage,
				tx.Successful,
				tx.Version,
				tx.RecentBlockhash,
				tx.NumReadonlySignedAccounts,
				tx.NumReadonlyUnsignedAccounts,
				tx.NumRequiredSignatures,
				tx.UpdatedAt,
				tx.CreatedAt,
			)
		}

		_, err = tx.Exec(sql, values...)
		if err != nil {
			return fmt.Errorf("error batch inserting transactions: %v", err)
		}
	}

	// Save transaction signatures in batches
	if len(block.TransactionSignatures) > 0 {
		columns := []string{"signature", "slot", "transaction_index", "updated_at", "created_at"}
		sql := generateBatchInsertSQL("transactions_signatures", columns, len(block.TransactionSignatures))

		values := make([]interface{}, 0, len(block.TransactionSignatures)*len(columns))
		for _, sig := range block.TransactionSignatures {
			values = append(values,
				sig.Signature,
				sig.Slot,
				sig.TransactionIndex,
				sig.UpdatedAt,
				sig.CreatedAt,
			)
		}

		_, err = tx.Exec(sql, values...)
		if err != nil {
			return fmt.Errorf("error batch inserting transaction signatures: %v", err)
		}
	}

	// Save transaction instructions in batches
	// if len(block.TransactionInstructions) > 0 {
	// 	columns := []string{
	// 		"slot", "transaction_index", "instruction_index", "program_id",
	// 		"program_id_index", "stack_height", "data", "updated_at", "created_at",
	// 	}
	// 	sql := generateBatchInsertSQL("transaction_instructions", columns, len(block.TransactionInstructions))

	// 	values := make([]interface{}, 0, len(block.TransactionInstructions)*len(columns))
	// 	for _, inst := range block.TransactionInstructions {
	// 		// Convert instruction data to base58
	// 		dataBase58 := base58.Encode(inst.Data)

	// 		values = append(values,
	// 			inst.Slot,
	// 			inst.TransactionIndex,
	// 			inst.InstructionIndex,
	// 			inst.ProgramId,
	// 			inst.ProgramIdIndex,
	// 			inst.StackHeight,
	// 			dataBase58,
	// 			inst.UpdatedAt,
	// 			inst.CreatedAt,
	// 		)
	// 	}

	// 	_, err = tx.Exec(sql, values...)
	// 	if err != nil {
	// 		return fmt.Errorf("error batch inserting transaction instructions: %v", err)
	// 	}
	// }

	// // Save transaction logs in batches
	// if len(block.TransactionLogs) > 0 {
	// 	columns := []string{
	// 		"slot", "transaction_index", "log_index", "log",
	// 		"level", "program_id", "updated_at", "created_at",
	// 	}
	// 	sql := generateBatchInsertSQL("transaction_logs", columns, len(block.TransactionLogs))

	// 	values := make([]interface{}, 0, len(block.TransactionLogs)*len(columns))
	// 	for _, log := range block.TransactionLogs {
	// 		values = append(values,
	// 			log.Slot,
	// 			log.TransactionIndex,
	// 			log.LogIndex,
	// 			log.Log,
	// 			log.Level,
	// 			log.ProgramId,
	// 			log.UpdatedAt,
	// 			log.CreatedAt,
	// 		)
	// 	}

	// 	_, err = tx.Exec(sql, values...)
	// 	if err != nil {
	// 		return fmt.Errorf("error batch inserting transaction logs: %v", err)
	// 	}
	// }

	// // Save transaction accounts in batches
	// if len(block.TransactionAccounts) > 0 {
	// 	columns := []string{
	// 		"slot", "transaction_index", "account_index", "pubkey",
	// 		"account_address", "is_signer", "is_writable", "pre_balance",
	// 		"post_balance", "balance_change", "source", "rent_epoch_change",
	// 		"updated_at", "created_at",
	// 	}
	// 	sql := generateBatchInsertSQL("transaction_accounts", columns, len(block.TransactionAccounts))

	// 	values := make([]interface{}, 0, len(block.TransactionAccounts)*len(columns))
	// 	for _, acc := range block.TransactionAccounts {
	// 		values = append(values,
	// 			acc.Slot,
	// 			acc.TransactionIndex,
	// 			acc.AccountIndex,
	// 			acc.Pubkey,
	// 			acc.AccountAddress,
	// 			acc.IsSigner,
	// 			acc.IsWritable,
	// 			acc.PreBalance,
	// 			acc.PostBalance,
	// 			acc.BalanceChange,
	// 			acc.Source,
	// 			acc.RentEpochChange,
	// 			acc.UpdatedAt,
	// 			acc.CreatedAt,
	// 		)
	// 	}

	// 	_, err = tx.Exec(sql, values...)
	// 	if err != nil {
	// 		return fmt.Errorf("error batch inserting transaction accounts: %v", err)
	// 	}
	// }

	// // Save transaction token balances in batches
	// if len(block.TransactionTokenBalances) > 0 {
	// 	columns := []string{
	// 		"slot", "transaction_index", "account_index", "mint", "owner",
	// 		"ui_token_amount", "amount", "decimals", "pre_amount", "pre_ui_amount",
	// 		"post_amount", "post_ui_amount", "updated_at", "created_at",
	// 	}
	// 	sql := generateBatchInsertSQL("transaction_token_balances", columns, len(block.TransactionTokenBalances))

	// 	values := make([]interface{}, 0, len(block.TransactionTokenBalances)*len(columns))
	// 	for _, bal := range block.TransactionTokenBalances {
	// 		values = append(values,
	// 			bal.Slot,
	// 			bal.TransactionIndex,
	// 			bal.AccountIndex,
	// 			bal.Mint,
	// 			bal.Owner,
	// 			bal.UiTokenAmount,
	// 			bal.Amount,
	// 			bal.Decimals,
	// 			bal.PreAmount,
	// 			bal.PreUiAmount,
	// 			bal.PostAmount,
	// 			bal.PostUiAmount,
	// 			bal.UpdatedAt,
	// 			bal.CreatedAt,
	// 		)
	// 	}

	// 	_, err = tx.Exec(sql, values...)
	// 	if err != nil {
	// 		return fmt.Errorf("error batch inserting transaction token balances: %v", err)
	// 	}
	// }

	// Commit transaction
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %v", err)
	}

	return nil
}
