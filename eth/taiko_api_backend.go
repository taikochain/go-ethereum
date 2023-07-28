package eth

import (
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
)

// TaikoAPIBackend handles l2 node related RPC calls.
type TaikoAPIBackend struct {
	eth *Ethereum
}

// NewTaikoAPIBackend creates a new TaikoAPIBackend instance.
func NewTaikoAPIBackend(eth *Ethereum) *TaikoAPIBackend {
	return &TaikoAPIBackend{
		eth: eth,
	}
}

// HeadL1Origin returns the latest L2 block's corresponding L1 origin.
func (s *TaikoAPIBackend) HeadL1Origin() (*rawdb.L1Origin, error) {
	blockID, err := rawdb.ReadHeadL1Origin(s.eth.ChainDb())
	if err != nil {
		return nil, err
	}

	if blockID == nil {
		return nil, ethereum.NotFound
	}

	l1Origin, err := rawdb.ReadL1Origin(s.eth.ChainDb(), blockID)
	if err != nil {
		return nil, err
	}

	if l1Origin == nil {
		return nil, ethereum.NotFound
	}

	return l1Origin, nil
}

// L1OriginByID returns the L2 block's corresponding L1 origin.
func (s *TaikoAPIBackend) L1OriginByID(blockID *math.HexOrDecimal256) (*rawdb.L1Origin, error) {
	l1Origin, err := rawdb.ReadL1Origin(s.eth.ChainDb(), (*big.Int)(blockID))
	if err != nil {
		return nil, err
	}

	if l1Origin == nil {
		return nil, ethereum.NotFound
	}

	return l1Origin, nil
}

// TxPoolContent retrieves the transaction pool content with the given upper limits.
func (s *TaikoAPIBackend) TxPoolContent(
	beneficiary common.Address,
	baseFee uint64,
	maxTransactionsPerBlock uint64,
	blockMaxGasUsed uint64,
	maxBytesPerTxList uint64,
	locals []string,
	maxTransactions uint64,
) ([]types.Transactions, error) {
	log.Debug(
		"Fetching L2 pending transactions finished",
		"beneficiary", beneficiary,
		"baseFee", baseFee,
		"maxTransactionsPerBlock", maxTransactionsPerBlock,
		"blockMaxGasUsed", blockMaxGasUsed,
		"maxBytesPerTxList", maxBytesPerTxList,
		"maxTransactions", maxTransactions,
		"locals", locals,
	)

	return s.eth.Miner().BuildTransactionsLists(
		beneficiary,
		new(big.Int).SetUint64(baseFee),
		maxTransactionsPerBlock,
		blockMaxGasUsed,
		maxBytesPerTxList,
		locals,
		maxTransactions,
	)
}
