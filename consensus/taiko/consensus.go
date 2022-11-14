package taiko

import (
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	ErrOlderBlockTime = errors.New("timestamp older than parent")
	ErrUnclesNotEmpty = errors.New("uncles not empty")
	ErrBaseFeeNotZero = errors.New("base fee not zero")
)

// Taiko is a consensus engine used by L2 rollup.
type Taiko struct{}

var defaultTaiko = new(Taiko)

func New() *Taiko {
	return defaultTaiko
}

// check all method stubs for interface `Engine` without affect performance.
var _ consensus.Engine = (*Taiko)(nil)

// Author retrieves the Ethereum address of the account that minted the given
// block, who proposes the block (not the prover).
func (t *Taiko) Author(header *types.Header) (common.Address, error) {
	return header.Coinbase, nil
}

// VerifyHeader checks whether a header conforms to the consensus rules of a
// given engine. Verifying the seal may be done optionally here, or explicitly
// via the VerifySeal method.
func (t *Taiko) VerifyHeader(chain consensus.ChainHeaderReader, header *types.Header, seal bool) error {
	// Short circuit if the header is known, or its parent not
	number := header.Number.Uint64()
	if chain.GetHeader(header.Hash(), number) != nil {
		return nil
	}
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	// Sanity checks passed, do a proper verification
	return t.verifyHeader(chain, header, parent, seal, time.Now().Unix())
}

// VerifyHeaders is similar to VerifyHeader, but verifies a batch of headers
// concurrently. The method returns a quit channel to abort the operations and
// a results channel to retrieve the async verifications (the order is that of
// the input slice).
func (t *Taiko) VerifyHeaders(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool) (chan<- struct{}, <-chan error) {
	// Spawn as many workers as allowed threads
	workers := runtime.GOMAXPROCS(0)
	if len(headers) < workers {
		workers = len(headers)
	}

	// Create a task channel and spawn the verifiers
	var (
		inputs  = make(chan int)
		done    = make(chan int, workers)
		errors  = make([]error, len(headers))
		abort   = make(chan struct{})
		unixNow = time.Now().Unix()
	)
	for i := 0; i < workers; i++ {
		go func() {
			for index := range inputs {
				errors[index] = t.verifyHeaderWorker(chain, headers, seals, index, unixNow)
				done <- index
			}
		}()
	}

	errorsOut := make(chan error, len(headers))
	go func() {
		defer close(inputs)
		var (
			in, out = 0, 0
			checked = make([]bool, len(headers))
			inputs  = inputs
		)
		for {
			select {
			case inputs <- in:
				if in++; in == len(headers) {
					// Reached end of headers. Stop sending to workers.
					inputs = nil
				}
			case index := <-done:
				for checked[index] = true; checked[out]; out++ {
					errorsOut <- errors[out]
					if out == len(headers)-1 {
						return
					}
				}
			case <-abort:
				return
			}
		}
	}()
	return abort, errorsOut
}

func (t *Taiko) verifyHeader(chain consensus.ChainHeaderReader, header, parent *types.Header, seal bool, unixNow int64) error {
	if header.Time > uint64(unixNow) {
		return consensus.ErrFutureBlock
	}

	// Ensure that the header's extra-data section is of a reasonable size (<= 32 bytes)
	if uint64(len(header.Extra)) > params.MaximumExtraDataSize {
		return fmt.Errorf("extra-data too long: %d > %d", len(header.Extra), params.MaximumExtraDataSize)
	}

	// Timestamp should later than or equal to parent (when many L2 blocks included in one L1 block)
	if header.Time < parent.Time {
		return ErrOlderBlockTime
	}

	// Verify that the block number is parent's +1
	if diff := new(big.Int).Sub(header.Number, parent.Number); diff.Cmp(big.NewInt(1)) != 0 {
		return consensus.ErrInvalidNumber
	}

	// Difficulty should always be zero
	if header.Difficulty != nil && header.Difficulty.Cmp(common.Big0) != 0 {
		return fmt.Errorf("invalid difficulty: have %v, want %v", header.Difficulty, common.Big0)
	}

	// Verify that the gas limit is <= 2^63-1
	if header.GasLimit > params.MaxGasLimit {
		return fmt.Errorf("invalid gasLimit: have %v, max %v", header.GasLimit, params.MaxGasLimit)
	}

	// Verify that the gasUsed is <= gasLimit
	if header.GasUsed > header.GasLimit {
		return fmt.Errorf("invalid gasUsed: have %d, gasLimit %d", header.GasUsed, header.GasLimit)
	}

	// Uncles should be empty
	if header.UncleHash != types.CalcUncleHash(nil) {
		return ErrUnclesNotEmpty
	}

	// Verify BaseFee not present before EIP-1559 fork.
	if header.BaseFee != nil {
		return ErrBaseFeeNotZero
	}

	// TODO: check baseFee when EIP-1559 is enabled.

	return nil
}

func (t *Taiko) verifyHeaderWorker(chain consensus.ChainHeaderReader, headers []*types.Header, seals []bool, index int, unixNow int64) error {
	var parent *types.Header
	if index == 0 {
		parent = chain.GetHeader(headers[0].ParentHash, headers[0].Number.Uint64()-1)
	} else if headers[index-1].Hash() == headers[index].ParentHash {
		parent = headers[index-1]
	}
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	return t.verifyHeader(chain, headers[index], parent, seals[index], unixNow)
}

// VerifyUncles verifies that the given block's uncles conform to the consensus
// rules of a given engine.
//
// always returning an error for any uncles as this consensus mechanism doesn't permit uncles.
func (t *Taiko) VerifyUncles(chain consensus.ChainReader, block *types.Block) error {
	if len(block.Uncles()) > 0 {
		return ErrUnclesNotEmpty
	}

	return nil
}

// Prepare initializes the consensus fields of a block header according to the
// rules of a particular engine. The changes are executed inline.
func (t *Taiko) Prepare(chain consensus.ChainHeaderReader, header *types.Header) error {
	parent := chain.GetHeader(header.ParentHash, header.Number.Uint64()-1)
	if parent == nil {
		return consensus.ErrUnknownAncestor
	}
	header.Difficulty = common.Big0
	return nil
}

// Finalize runs any post-transaction state modifications (e.g. block rewards)
// but does not assemble the block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (t *Taiko) Finalize(chain consensus.ChainHeaderReader, header *types.Header, state *state.StateDB, txs []*types.Transaction, uncles []*types.Header) {
	// no block rewards in l2
	header.Root = state.IntermediateRoot(true)
	header.UncleHash = types.CalcUncleHash(nil)
	header.Difficulty = common.Big0
}

// FinalizeAndAssemble runs any post-transaction state modifications (e.g. block
// rewards) and assembles the final block.
//
// Note: The block header and state database might be updated to reflect any
// consensus rules that happen at finalization (e.g. block rewards).
func (t *Taiko) FinalizeAndAssemble(chain consensus.ChainHeaderReader, header *types.Header, state *state.StateDB, txs []*types.Transaction, uncles []*types.Header, receipts []*types.Receipt) (*types.Block, error) {
	// Finalize block
	t.Finalize(chain, header, state, txs, uncles)
	return types.NewBlock(header, txs, nil /* ignore uncles */, receipts, trie.NewStackTrie(nil)), nil
}

// Seal generates a new sealing request for the given input block and pushes
// the result into the given channel.
//
// Note, the method returns immediately and will send the result async. More
// than one result may also be returned depending on the consensus algorithm.
func (t *Taiko) Seal(chain consensus.ChainHeaderReader, block *types.Block, results chan<- *types.Block, stop <-chan struct{}) error {
	header := block.Header()

	// Sealing the genesis block is not supported
	number := header.Number.Uint64()
	if number == 0 {
		return consensus.ErrInvalidNumber
	}

	select {
	case results <- block.WithSeal(header):
	case <-stop:
		return nil
	default:
		log.Warn("Sealing result is not read by miner", "sealHash", t.SealHash(header))
	}

	return nil
}

// SealHash returns the hash of a block prior to it being sealed.
func (t *Taiko) SealHash(header *types.Header) common.Hash {
	// Keccak(rlp(header))
	return header.Hash()
}

// CalcDifficulty is the difficulty adjustment algorithm. It returns the difficulty
// that a new block should have.
func (t *Taiko) CalcDifficulty(chain consensus.ChainHeaderReader, time uint64, parent *types.Header) *big.Int {
	return common.Big0
}

// APIs returns the RPC APIs this consensus engine provides.
func (t *Taiko) APIs(chain consensus.ChainHeaderReader) []rpc.API {
	return nil
}

// Close terminates any background threads maintained by the consensus engine.
func (t *Taiko) Close() error {
	return nil
}
