package eth

import (
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/core"
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
	maxTransactionsPerBlock uint64,
	blockMaxGasLimit uint64,
	maxBytesPerTxList uint64,
	minTxGasLimit uint64,
	locals []string,
) ([]types.Transactions, error) {
	pending := s.eth.TxPool().Pending(false)

	log.Debug(
		"Fetching L2 pending transactions finished",
		"length", core.PoolContent(pending).Len(),
		"maxTransactionsPerBlock", maxTransactionsPerBlock,
		"blockMaxGasLimit", blockMaxGasLimit,
		"maxBytesPerTxList", maxBytesPerTxList,
		"minTxGasLimit", minTxGasLimit,
		"locals", locals,
	)

	contentSplitter, err := core.NewPoolContentSplitter(
		s.eth.BlockChain().Config().ChainID,
		maxTransactionsPerBlock,
		blockMaxGasLimit,
		maxBytesPerTxList,
		minTxGasLimit,
		locals,
	)
	if err != nil {
		return nil, err
	}

	var (
		txsCount = 0
		txLists  []types.Transactions
	)
	for _, splittedTxs := range contentSplitter.Split(filterTxs(pending)) {
		if txsCount+splittedTxs.Len() < int(maxTransactionsPerBlock) {
			txLists = append(txLists, splittedTxs)
			txsCount += splittedTxs.Len()
			continue
		}

		txLists = append(txLists, splittedTxs[0:(int(maxTransactionsPerBlock)-txsCount)])
		break
	}

	return txLists, nil
}

func filterTxs(pendings map[common.Address]types.Transactions) map[common.Address]types.Transactions {
	executableTxs := make(map[common.Address]types.Transactions)

	for addr, txs := range pendings {
		pendingTxs := make(types.Transactions, 0)
		for _, tx := range txs {
			// Check baseFee, should not be zero
			if tx.GasFeeCap().Uint64() == 0 {
				break
			}

			// If this tx is a transfer and with high gas limit, ignore it.
			if len(tx.Data()) == 0 && tx.Gas() > 100_000 {
				break
			}

			pendingTxs = append(pendingTxs, tx)
		}

		if len(pendingTxs) > 0 {
			executableTxs[addr] = pendingTxs
		}
	}

	return executableTxs
}
