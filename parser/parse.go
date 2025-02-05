package parser

import (
	"fmt"
	pb "goblockstore/proto"
	"time"

	"github.com/mr-tron/base58"
)

// ParseBlock parses a Yellowstone gRPC block into our structured format
func ParseBlock(block *pb.SubscribeUpdateBlock) (*ParsedBlock, error) {
	now := time.Now()
	blockTime := time.Unix(block.BlockTime.GetTimestamp(), 0)

	result := &ParsedBlock{
		Block: Block{
			Slot:              block.Slot,
			Blockhash:         block.Blockhash,
			ParentSlot:        block.ParentSlot,
			PreviousBlockhash: block.ParentBlockhash,
			BlockHeight:       block.BlockHeight.GetBlockHeight(),
			BlockTime:         blockTime,
			CreatedAt:         now,
			UpdatedAt:         now,
		},
	}

	// Parse rewards
	// if rewards := block.GetRewards().GetRewards(); rewards != nil {
	// 	for i, reward := range rewards {
	// 		blockReward := BlockReward{
	// 			Slot:        result.Block.Slot,
	// 			RewardIndex: i,
	// 			Lamports:    reward.Lamports,
	// 			PostBalance: reward.PostBalance,
	// 			Pubkey:      reward.Pubkey,
	// 			RewardType:  reward.RewardType.String(),
	// 			CreatedAt:   now,
	// 			UpdatedAt:   now,
	// 		}
	// 		if reward.Commission != "" {
	// 			var commission int64
	// 			fmt.Sscanf(reward.Commission, "%d", &commission)
	// 			blockReward.Commission = &commission
	// 		}
	// 		result.BlockRewards = append(result.BlockRewards, blockReward)
	// 	}
	// }

	// Parse transactions
	if txs := block.GetTransactions(); txs != nil {
		for i, tx := range txs {
			if tx.IsVote {
				continue
			}
			transaction := Transaction{
				Slot:             result.Block.Slot,
				TransactionIndex: i,
				BlockTime:        blockTime,
				BlockHash:        result.Block.Blockhash,
				Fee:              tx.Meta.Fee,
				CreatedAt:        now,
				UpdatedAt:        now,
				Successful:       tx.Meta.Err == nil,
			}

			if tx.Meta != nil {
				if tx.Meta.Err != nil {
					errStr := string(tx.Meta.Err.Err)
					transaction.Err = &TransactionError{Data: errStr}
				}
				if tx.Meta.ComputeUnitsConsumed != nil {
					transaction.ComputeUnitsConsumed = *tx.Meta.ComputeUnitsConsumed
				}
			}

			if tx.Transaction != nil && tx.Transaction.Message != nil {
				if header := tx.Transaction.Message.Header; header != nil {
					transaction.NumRequiredSignatures = header.NumRequiredSignatures
					transaction.NumReadonlySignedAccounts = header.NumReadonlySignedAccounts
					transaction.NumReadonlyUnsignedAccounts = header.NumReadonlyUnsignedAccounts
				}
				if tx.Transaction.Message.RecentBlockhash != nil {
					transaction.RecentBlockhash = base58.Encode(tx.Transaction.Message.RecentBlockhash)
				}

				// Parse instructions
				for j, inst := range tx.Transaction.Message.Instructions {
					instruction := Instruction{
						Slot:             result.Block.Slot,
						TransactionIndex: i,
						InstructionIndex: j,
						ProgramIdIndex:   int(inst.ProgramIdIndex),
						Data:             inst.Data,
						CreatedAt:        now,
						UpdatedAt:        now,
					}
					if tx.Transaction.Message.AccountKeys != nil && int(inst.ProgramIdIndex) < len(tx.Transaction.Message.AccountKeys) {
						instruction.ProgramId = base58.Encode(tx.Transaction.Message.AccountKeys[inst.ProgramIdIndex])
					}
					result.TransactionInstructions = append(result.TransactionInstructions, instruction)
				}

				// Parse accounts
				if tx.Meta != nil && tx.Transaction.Message.AccountKeys != nil {
					for j, key := range tx.Transaction.Message.AccountKeys {
						account := TransactionAccount{
							Slot:             result.Block.Slot,
							TransactionIndex: i,
							AccountIndex:     j,
							Pubkey:           base58.Encode(key),
							AccountAddress:   base58.Encode(key),
							IsSigner:         j < int(tx.Transaction.Message.Header.NumRequiredSignatures),
							IsWritable: j < int(tx.Transaction.Message.Header.NumRequiredSignatures-tx.Transaction.Message.Header.NumReadonlySignedAccounts) ||
								(j >= int(tx.Transaction.Message.Header.NumRequiredSignatures) && j < len(tx.Transaction.Message.AccountKeys)-int(tx.Transaction.Message.Header.NumReadonlyUnsignedAccounts)),
							CreatedAt: now,
							UpdatedAt: now,
						}

						if j < len(tx.Meta.PreBalances) {
							account.PreBalance = tx.Meta.PreBalances[j]
						}
						if j < len(tx.Meta.PostBalances) {
							account.PostBalance = tx.Meta.PostBalances[j]
							if account.PreBalance > 0 {
								account.BalanceChange = int64(account.PostBalance) - int64(account.PreBalance)
							}
						}

						result.TransactionAccounts = append(result.TransactionAccounts, account)
					}
				}

				// Parse token balances
				if tx.Meta != nil && tx.Meta.PreTokenBalances != nil {
					preBalMap := make(map[int]*pb.TokenBalance)
					for _, bal := range tx.Meta.PreTokenBalances {
						preBalMap[int(bal.AccountIndex)] = bal
					}

					postBalMap := make(map[int]*pb.TokenBalance)
					for _, bal := range tx.Meta.PostTokenBalances {
						postBalMap[int(bal.AccountIndex)] = bal
					}

					// Combine pre and post balances
					for idx, preBal := range preBalMap {
						tokenBal := TransactionTokenBalance{
							Slot:             result.Block.Slot,
							TransactionIndex: i,
							AccountIndex:     idx,
							Mint:             preBal.Mint,
							Owner:            preBal.Owner,
							Decimals:         int(preBal.UiTokenAmount.Decimals),
							PreAmount:        preBal.UiTokenAmount.Amount,
							PreUiAmount:      preBal.UiTokenAmount.UiAmount,
							CreatedAt:        now,
							UpdatedAt:        now,
						}

						if postBal, exists := postBalMap[idx]; exists {
							tokenBal.Amount = postBal.UiTokenAmount.Amount
							tokenBal.UiTokenAmount = postBal.UiTokenAmount.UiAmount
							tokenBal.PostAmount = postBal.UiTokenAmount.Amount
							tokenBal.PostUiAmount = postBal.UiTokenAmount.UiAmount
						}

						result.TransactionTokenBalances = append(result.TransactionTokenBalances, tokenBal)
					}

					// Add any post balances that didn't have pre balances
					for idx, postBal := range postBalMap {
						if _, exists := preBalMap[idx]; !exists {
							tokenBal := TransactionTokenBalance{
								Slot:             result.Block.Slot,
								TransactionIndex: i,
								AccountIndex:     idx,
								Mint:             postBal.Mint,
								Owner:            postBal.Owner,
								Amount:           postBal.UiTokenAmount.Amount,
								UiTokenAmount:    postBal.UiTokenAmount.UiAmount,
								Decimals:         int(postBal.UiTokenAmount.Decimals),
								PostAmount:       postBal.UiTokenAmount.Amount,
								PostUiAmount:     postBal.UiTokenAmount.UiAmount,
								CreatedAt:        now,
								UpdatedAt:        now,
							}
							result.TransactionTokenBalances = append(result.TransactionTokenBalances, tokenBal)
						}
					}
				}
			}

			// Parse signatures
			if tx.Transaction != nil {
				for _, sig := range tx.Transaction.Signatures {
					result.TransactionSignatures = append(result.TransactionSignatures, TransactionSignature{
						Signature:        base58.Encode(sig),
						Slot:             result.Block.Slot,
						TransactionIndex: i,
						CreatedAt:        now,
						UpdatedAt:        now,
					})
				}
			}

			// Parse logs
			if tx.Meta != nil && tx.Meta.LogMessages != nil {
				for j, log := range tx.Meta.LogMessages {
					txLog := TransactionLog{
						Slot:             result.Block.Slot,
						TransactionIndex: i,
						LogIndex:         j,
						Log:              log,
						CreatedAt:        now,
						UpdatedAt:        now,
					}
					// Extract program ID from log if possible
					if len(result.TransactionInstructions) > 0 {
						txLog.ProgramId = result.TransactionInstructions[0].ProgramId
					}
					result.TransactionLogs = append(result.TransactionLogs, txLog)
				}
			}

			result.Transactions = append(result.Transactions, transaction)
		}
	}

	result.Block.TransactionCount = len(result.Transactions)
	result.Block.Successful = true

	return result, nil
}

// parseUint64 parses a string into a uint64
func parseUint64(s string) (uint64, error) {
	var result uint64
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}

// encodeBase58 encodes bytes to base58 string
func encodeBase58(b []byte) string {
	return base58.Encode(b)
}

// decodeBase58 decodes a base58 string to bytes
func decodeBase58(s string) ([]byte, error) {
	return base58.Decode(s)
}
