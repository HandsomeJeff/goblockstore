package parser

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// Block represents a parsed Solana block
type Block struct {
	Slot              uint64     `db:"slot"`
	ParentSlot        uint64     `db:"parent_slot"`
	BlockTime         time.Time  `db:"block_time"`
	BlockHeight       uint64     `db:"block_height"`
	Blockhash         string     `db:"blockhash"`
	PreviousBlockhash string     `db:"previous_blockhash"`
	TransactionCount  int        `db:"transaction_count"`
	Successful        bool       `db:"successful"`
	UpdatedAt         time.Time  `db:"updated_at"`
	CreatedAt         time.Time  `db:"created_at"`
	DeletedAt         *time.Time `db:"deleted_at"`
}

// BlockReward represents a reward in a Solana block
type BlockReward struct {
	Slot        uint64     `db:"slot"`
	RewardIndex int        `db:"reward_index"`
	Pubkey      string     `db:"pubkey"`
	Lamports    int64      `db:"lamports"`
	PostBalance uint64     `db:"post_balance"`
	RewardType  string     `db:"reward_type"`
	Commission  *int64     `db:"commission"`
	UpdatedAt   time.Time  `db:"updated_at"`
	CreatedAt   time.Time  `db:"created_at"`
	DeletedAt   *time.Time `db:"deleted_at"`
}

type Error struct {
	Data any `json:"data,omitempty"`
}

// TransactionError represents an error in a Solana transaction
type TransactionError struct {
	Data interface{} `json:"data,omitempty"`
}

// Value implements the driver.Valuer interface for database serialization
func (e *TransactionError) Value() (driver.Value, error) {
	if e == nil {
		return nil, nil
	}
	return json.Marshal(e)
}

// Scan implements the sql.Scanner interface for database deserialization
func (e *TransactionError) Scan(value interface{}) error {
	if value == nil {
		*e = TransactionError{}
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("expected []byte, got %T", value)
	}

	return json.Unmarshal(bytes, e)
}

// Transaction represents a parsed Solana transaction
type Transaction struct {
	Slot                        uint64            `db:"slot"`
	TransactionIndex            int               `db:"transaction_index"`
	BlockTime                   time.Time         `db:"block_time"`
	BlockHash                   string            `db:"block_hash"`
	Fee                         uint64            `db:"fee"`
	ComputeUnitsConsumed        uint64            `db:"compute_units_consumed"`
	ComputeUnitsPrice           uint64            `db:"compute_units_price"`
	Err                         *TransactionError `db:"err"`
	ErrMessage                  *string           `db:"err_message"`
	ErrStack                    *string           `db:"err_stack"`
	ErrInstructionIndex         *int64            `db:"err_instruction_index"`
	ErrCustomCode               *int64            `db:"err_custom_code"`
	ErrCustomMessage            *string           `db:"err_custom_message"`
	Successful                  bool              `db:"successful"`
	Version                     string            `db:"version"`
	RecentBlockhash             string            `db:"recent_blockhash"`
	NumReadonlySignedAccounts   uint32            `db:"num_readonly_signed_accounts"`
	NumReadonlyUnsignedAccounts uint32            `db:"num_readonly_unsigned_accounts"`
	NumRequiredSignatures       uint32            `db:"num_required_signatures"`
	UpdatedAt                   time.Time         `db:"updated_at"`
	CreatedAt                   time.Time         `db:"created_at"`
	DeletedAt                   *time.Time        `db:"deleted_at"`
}

// Instruction represents a parsed Solana instruction
type Instruction struct {
	Slot             uint64     `db:"slot"`
	TransactionIndex int        `db:"transaction_index"`
	InstructionIndex int        `db:"instruction_index"`
	ProgramId        string     `db:"program_id"`
	ProgramIdIndex   int        `db:"program_id_index"`
	StackHeight      int64      `db:"stack_height"`
	Data             []byte     `db:"data"`
	UpdatedAt        time.Time  `db:"updated_at"`
	CreatedAt        time.Time  `db:"created_at"`
	DeletedAt        *time.Time `db:"deleted_at"`
}

// TransactionLog represents a log entry in a Solana transaction
type TransactionLog struct {
	Slot             uint64     `db:"slot"`
	TransactionIndex int        `db:"transaction_index"`
	LogIndex         int        `db:"log_index"`
	Log              string     `db:"log"`
	Level            string     `db:"level"`
	ProgramId        string     `db:"program_id"`
	UpdatedAt        time.Time  `db:"updated_at"`
	CreatedAt        time.Time  `db:"created_at"`
	DeletedAt        *time.Time `db:"deleted_at"`
}

// TransactionAccount represents an account involved in a Solana transaction
type TransactionAccount struct {
	Slot             uint64     `db:"slot"`
	TransactionIndex int        `db:"transaction_index"`
	AccountIndex     int        `db:"account_index"`
	Pubkey           string     `db:"pubkey"`
	AccountAddress   string     `db:"account_address"`
	IsSigner         bool       `db:"is_signer"`
	IsWritable       bool       `db:"is_writable"`
	PreBalance       uint64     `db:"pre_balance"`
	PostBalance      uint64     `db:"post_balance"`
	BalanceChange    int64      `db:"balance_change"`
	Source           string     `db:"source"`
	RentEpochChange  int64      `db:"rent_epoch_change"`
	UpdatedAt        time.Time  `db:"updated_at"`
	CreatedAt        time.Time  `db:"created_at"`
	DeletedAt        *time.Time `db:"deleted_at"`
}

// TransactionInnerInstruction represents an inner instruction in a Solana transaction
type TransactionInnerInstruction struct {
	Slot                  uint64     `db:"slot"`
	SlotIndex             int        `db:"slot_index"`
	InstructionIndex      int        `db:"instruction_index"`
	InnerInstructionIndex int        `db:"inner_instruction_index"`
	ProgramId             string     `db:"program_id"`
	ProgramIdIndex        int        `db:"program_id_index"`
	Data                  []byte     `db:"data"`
	UpdatedAt             time.Time  `db:"updated_at"`
	CreatedAt             time.Time  `db:"created_at"`
	DeletedAt             *time.Time `db:"deleted_at"`
}

// TransactionTokenBalance represents a token balance in a Solana transaction
type TransactionTokenBalance struct {
	Slot             uint64     `db:"slot"`
	TransactionIndex int        `db:"transaction_index"`
	AccountIndex     int        `db:"account_index"`
	Mint             string     `db:"mint"`
	Owner            string     `db:"owner"`
	UiTokenAmount    float64    `db:"ui_token_amount"`
	Amount           string     `db:"amount"`
	Decimals         int        `db:"decimals"`
	PreAmount        string     `db:"pre_amount"`
	PreUiAmount      float64    `db:"pre_ui_amount"`
	PostAmount       string     `db:"post_amount"`
	PostUiAmount     float64    `db:"post_ui_amount"`
	UpdatedAt        time.Time  `db:"updated_at"`
	CreatedAt        time.Time  `db:"created_at"`
	DeletedAt        *time.Time `db:"deleted_at"`
}

// TransactionSignature represents a signature in a Solana transaction
type TransactionSignature struct {
	Signature        string     `db:"signature"`
	Slot             uint64     `db:"slot"`
	TransactionIndex int        `db:"transaction_index"`
	UpdatedAt        time.Time  `db:"updated_at"`
	CreatedAt        time.Time  `db:"created_at"`
	DeletedAt        *time.Time `db:"deleted_at"`
}

// ParsedBlock represents all data parsed from a Solana block
type ParsedBlock struct {
	Block                        Block
	BlockRewards                 []BlockReward
	Transactions                 []Transaction
	TransactionLogs              []TransactionLog
	TransactionAccounts          []TransactionAccount
	TransactionInstructions      []Instruction
	TransactionInnerInstructions []TransactionInnerInstruction
	TransactionTokenBalances     []TransactionTokenBalance
	TransactionSignatures        []TransactionSignature
}
