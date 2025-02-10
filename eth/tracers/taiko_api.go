package tracers

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"
)

type TaikoBackend interface {
	BlockChain() *core.BlockChain
}

// provingPreflightResult is the result of a proving preflight request.
type provingPreflightResult struct {
	Block             *types.Block                      `json:"block"`
	InitAccountProofs []*ethapi.AccountResult           `json:"initAccountProofs"`
	Contracts         map[common.Address]*hexutil.Bytes `json:"contracts"`
	AncestorHashes    map[uint64]common.Hash            `json:"ancestorHashes"`
	Error             string                            `json:"error,omitempty"`
}

// provingPreflightTask represents a single block preflight task.
type provingPreflightTask struct {
	statedb   *state.StateDB          // Intermediate state prepped for preflighting
	parent    *types.Block            // Parent block of the block to preflight
	block     *types.Block            // Block to preflight the transactions from
	release   StateReleaseFunc        // The function to release the held resource for this task
	preflight *provingPreflightResult // Preflight results produced by the task
}

// ProvingPreflights traces the blockchain and gets preflights from the start block to the end block
// and returns a subscription to the results. This function is intended to be
// used with long-running operations, as tracing a chain can be time-consuming.
//
// Parameters:
// - ctx: The context for the operation.
// - start: The starting block number for the trace.
// - end: The ending block number for the trace.
// - config: The configuration for the trace.
//
// Returns:
// - *rpc.Subscription: A subscription to the trace results.
// - error: An error if the operation fails, or if the end block is not after the start block.
//
// Note:
//   - The function requires a notifier to be present in the context, otherwise it
//     returns an error indicating that notifications are unsupported.
func (api *API) ProvingPreflights(ctx context.Context, start, end rpc.BlockNumber, config *TraceConfig) ([]*provingPreflightResult, error) {
	if start == 0 {
		return nil, fmt.Errorf("start block must be greater than 0")
	}
	// Adjust the start block to the parent one
	start -= 1

	from, err := api.blockByNumber(ctx, start)
	if err != nil {
		return nil, err
	}
	to, err := api.blockByNumber(ctx, end)
	if err != nil {
		return nil, err
	}
	if from.Number().Cmp(to.Number()) >= 0 {
		return nil, fmt.Errorf("end block (#%d) needs to come after start block (#%d)", end, start)
	}

	resCh := api.provingPreflights(ctx, from, to, config)
	var res []*provingPreflightResult
	for result := range resCh {
		res = append(res, result)
	}
	return res, nil
}

// provingPreflights traces the execution of blocks from `start` to `end` and returns a channel
// that streams the results of the preflight checks for each block. The function uses multiple
// goroutines to parallelize the tracing process.
//
// Parameters:
// - ctx: The context for the operation.
// - start: The starting block for tracing.
// - end: The ending block for tracing.
// - config: Configuration for tracing, including the tracer to use and reexec settings.
//
// Returns:
// - A channel that streams the results of the preflight checks for each block.
//
// The function performs the following steps:
// 1. Determines the number of blocks to trace and the number of threads to use.
// 2. Initializes the tracing configuration and state tracker.
// 3. Starts multiple goroutines to trace the blocks concurrently.
// 4. Feeds the blocks into the tracers and processes the results.
// 5. Streams the results back to the caller through the returned channel.
//
// The tracing process involves fetching the blocks, preparing the state, and tracing each
// transaction within the blocks. The results include the transaction trace results, account
// proofs, contract codes, and ancestor hashes.
func (api *API) provingPreflights(ctx context.Context, start, end *types.Block, config *TraceConfig) chan *provingPreflightResult {
	reexec := defaultTraceReexec
	if config != nil && config.Reexec != nil {
		reexec = *config.Reexec
	}
	blocks := int(end.NumberU64() - start.NumberU64())
	threads := runtime.NumCPU()
	if threads > blocks {
		threads = blocks
	}
	var (
		pend    = new(sync.WaitGroup)
		taskCh  = make(chan *provingPreflightTask, threads)
		resCh   = make(chan *provingPreflightTask, threads)
		tracker = newStateTracker(maximumPendingTraceStates, start.NumberU64())
	)
	for th := 0; th < threads; th++ {
		pend.Add(1)
		go func() {
			defer pend.Done()

			// Fetch and execute the block trace taskCh
			for task := range taskCh {
				touchedHashes := map[uint64]common.Hash{}
				hashFuncWrapper := func(hashFunc vm.GetHashFunc) vm.GetHashFunc {
					return func(n uint64) common.Hash {
						hash := hashFunc(n)
						touchedHashes[n] = hash
						return hash
					}
				}
				var (
					signer   = types.MakeSigner(api.backend.ChainConfig(), task.block.Number(), task.block.Time())
					blockCtx = core.NewEVMBlockContext(task.block.Header(), api.chainContext(ctx), nil)
				)
				blockCtx.GetHash = hashFuncWrapper(blockCtx.GetHash)
				// Trace all the transactions contained within
				for i, tx := range task.block.Transactions() {
					if i == 0 && api.backend.ChainConfig().Taiko {
						if err := tx.MarkAsAnchor(); err != nil {
							log.Warn("Mark anchor transaction error", "error", err)
							task.preflight.Error = err.Error()
							break
						}
					}
					msg, _ := core.TransactionToMessage(tx, signer, task.block.BaseFee())
					// CHANGE(taiko): decode the basefeeSharingPctg config from the extradata, and
					// add it to the Message, if its an ontake block.
					if api.backend.ChainConfig().IsOntake(task.block.Number()) {
						msg.BasefeeSharingPctg = core.DecodeOntakeExtraData(task.block.Header().Extra)
					}
					txctx := &Context{
						BlockHash:   task.block.Hash(),
						BlockNumber: task.block.Number(),
						TxIndex:     i,
						TxHash:      tx.Hash(),
					}
					_, err := api.traceTx(ctx, tx, msg, txctx, blockCtx, task.statedb, config)
					if err != nil {
						log.Warn("Tracing failed", "hash", tx.Hash(), "block", task.block.NumberU64(), "err", err)
						task.preflight.Error = err.Error()
						break
					}
				}
				// Tracing state is used up, queue it for de-referencing. Note the
				// state is the parent state of trace block, use block.number-1 as
				// the state number.
				tracker.releaseState(task.block.NumberU64()-1, task.release)

				// Retrieve the touched accounts from the state
				for addr, slots := range task.statedb.TouchedAccounts() {
					proof, code, err := api.getProof(ctx, addr, slots, task.parent, reexec)
					if err != nil {
						task.preflight.Error = err.Error()
						break
					}
					task.preflight.InitAccountProofs = append(task.preflight.InitAccountProofs, proof)
					if code != nil {
						task.preflight.Contracts[addr] = (*hexutil.Bytes)(&code)
					}
				}
				task.preflight.AncestorHashes = touchedHashes

				// Stream the result back to the result catcher or abort on teardown
				select {
				case resCh <- task:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	// Start a goroutine to feed all the blocks into the tracers
	go func() {
		var (
			logged  time.Time
			begin   = time.Now()
			number  uint64
			traced  uint64
			failed  error
			statedb *state.StateDB
			release StateReleaseFunc
		)
		// Ensure everything is properly cleaned up on any exit path
		defer func() {
			close(taskCh)
			pend.Wait()

			// Clean out any pending release functions of trace states.
			tracker.callReleases()

			// Log the chain result
			switch {
			case failed != nil:
				log.Warn("Chain tracing failed", "start", start.NumberU64(), "end", end.NumberU64(), "transactions", traced, "elapsed", time.Since(begin), "err", failed)
			case number < end.NumberU64():
				log.Warn("Chain tracing aborted", "start", start.NumberU64(), "end", end.NumberU64(), "abort", number, "transactions", traced, "elapsed", time.Since(begin))
			default:
				log.Info("Chain tracing finished", "start", start.NumberU64(), "end", end.NumberU64(), "transactions", traced, "elapsed", time.Since(begin))
			}
			close(resCh)
		}()
		// Feed all the blocks both into the tracer, as well as fast process concurrently
		for number = start.NumberU64(); number < end.NumberU64(); number++ {
			// Stop tracing if interruption was requested
			select {
			case <-ctx.Done():
				return
			default:
			}
			// Print progress logs if long enough time elapsed
			if time.Since(logged) > 8*time.Second {
				logged = time.Now()
				log.Info("Tracing chain segment", "start", start.NumberU64(), "end", end.NumberU64(), "current", number, "transactions", traced, "elapsed", time.Since(begin))
			}
			// Retrieve the parent block and target block for tracing.
			block, err := api.blockByNumber(ctx, rpc.BlockNumber(number))
			if err != nil {
				failed = err
				break
			}
			next, err := api.blockByNumber(ctx, rpc.BlockNumber(number+1))
			if err != nil {
				failed = err
				break
			}
			// Make sure the state creator doesn't go too far. Too many unprocessed
			// trace state may cause the oldest state to become stale(e.g. in
			// path-based scheme).
			if err = tracker.wait(number); err != nil {
				failed = err
				break
			}
			// Prepare the statedb for tracing. Don't use the live database for
			// tracing to avoid persisting state junks into the database. Switch
			// over to `preferDisk` mode only if the memory usage exceeds the
			// limit, the trie database will be reconstructed from scratch only
			// if the relevant state is available in disk.
			var preferDisk bool
			if statedb != nil {
				s1, s2, s3 := statedb.Database().TrieDB().Size()
				preferDisk = s1+s2+s3 > defaultTracechainMemLimit
			}
			statedb, release, err = api.backend.StateAtBlock(ctx, block, reexec, statedb, false, preferDisk)
			if err != nil {
				failed = err
				break
			}
			// Insert block's parent beacon block root in the state
			// as per EIP-4788.
			if beaconRoot := next.BeaconRoot(); beaconRoot != nil {
				context := core.NewEVMBlockContext(next.Header(), api.chainContext(ctx), nil)
				vmenv := vm.NewEVM(context, vm.TxContext{}, statedb, api.backend.ChainConfig(), vm.Config{})
				core.ProcessBeaconBlockRoot(*beaconRoot, vmenv, statedb)
			}
			// Insert parent hash in history contract.
			if api.backend.ChainConfig().IsPrague(next.Number(), next.Time()) {
				context := core.NewEVMBlockContext(next.Header(), api.chainContext(ctx), nil)
				vmenv := vm.NewEVM(context, vm.TxContext{}, statedb, api.backend.ChainConfig(), vm.Config{})
				core.ProcessParentBlockHash(next.ParentHash(), vmenv, statedb)
			}
			// Clean out any pending release functions of trace state. Note this
			// step must be done after constructing tracing state, because the
			// tracing state of block next depends on the parent state and construction
			// may fail if we release too early.
			tracker.callReleases()

			// Send the block over to the concurrent tracers (if not in the fast-forward phase)
			txs := next.Transactions()
			select {
			case taskCh <- &provingPreflightTask{
				statedb: statedb.Copy(),
				parent:  block,
				block:   next,
				release: release,
				preflight: &provingPreflightResult{
					Block:             next,
					InitAccountProofs: []*ethapi.AccountResult{},
					Contracts:         map[common.Address]*hexutil.Bytes{},
					AncestorHashes:    map[uint64]common.Hash{},
				},
			}:
			case <-ctx.Done():
				tracker.releaseState(number, release)
				return
			}
			traced += uint64(len(txs))
		}
	}()

	// Keep reading the trace results and stream them to result channel.
	retCh := make(chan *provingPreflightResult)
	go func() {
		defer close(retCh)
		var (
			next = start.NumberU64() + 1
			done = make(map[uint64]*provingPreflightResult)
		)
		for res := range resCh {
			// Queue up next received result
			result := res.preflight
			done[res.block.NumberU64()] = result

			// Stream completed traces to the result channel
			for result, ok := done[next]; ok; result, ok = done[next] {
				// It will be blocked in case the channel consumer doesn't take the
				// tracing result in time(e.g. the websocket connect is not stable)
				// which will eventually block the entire chain tracer. It's the
				// expected behavior to not waste node resources for a non-active user.
				retCh <- result
				delete(done, next)
				next++
			}
		}
	}()
	return retCh
}

// getProof returns the Merkle-proof for a given account and optionally some storage keys.
func (api *API) getProof(ctx context.Context, address common.Address, storageKeys []common.Hash, parent *types.Block, reexec uint64) (*ethapi.AccountResult, []byte, error) {
	var (
		storageProof = make([]ethapi.StorageResult, len(storageKeys))
	)

	header := parent.Header()

	statedb, release, err := api.backend.StateAtBlock(ctx, parent, reexec, nil, true, false)
	if statedb == nil || err != nil {
		return nil, nil, err
	}
	defer release()

	codeHash := statedb.GetCodeHash(address)
	storageRoot := statedb.GetStorageRoot(address)

	if len(storageKeys) > 0 {
		var storageTrie state.Trie
		if storageRoot != types.EmptyRootHash && storageRoot != (common.Hash{}) {
			id := trie.StorageTrieID(header.Root, crypto.Keccak256Hash(address.Bytes()), storageRoot)
			st, err := trie.NewStateTrie(id, statedb.Database().TrieDB())
			if err != nil {
				return nil, nil, err
			}
			storageTrie = st
		}
		// Create the proofs for the storageKeys.
		for i, key := range storageKeys {
			// Output key encoding is a bit special: if the input was a 32-byte hash, it is
			// returned as such. Otherwise, we apply the QUANTITY encoding mandated by the
			// JSON-RPC spec for getProof. This behavior exists to preserve backwards
			// compatibility with older client versions.
			outputKey := hexutil.Encode(key[:])
			if storageTrie == nil {
				storageProof[i] = ethapi.StorageResult{Key: outputKey, Value: &hexutil.Big{}, Proof: []string{}}
				continue
			}
			var proof proofList
			if err := storageTrie.Prove(crypto.Keccak256(key.Bytes()), &proof); err != nil {
				return nil, nil, err
			}
			value := (*hexutil.Big)(statedb.GetState(address, key).Big())
			storageProof[i] = ethapi.StorageResult{Key: outputKey, Value: value, Proof: proof}
		}
	}
	// Create the accountProof.
	tr, err := trie.NewStateTrie(trie.StateTrieID(header.Root), statedb.Database().TrieDB())
	if err != nil {
		return nil, nil, err
	}
	var accountProof proofList
	if err := tr.Prove(crypto.Keccak256(address.Bytes()), &accountProof); err != nil {
		return nil, nil, err
	}
	balance := statedb.GetBalance(address).ToBig()
	return &ethapi.AccountResult{
		Address:      address,
		AccountProof: accountProof,
		Balance:      (*hexutil.Big)(balance),
		CodeHash:     codeHash,
		Nonce:        hexutil.Uint64(statedb.GetNonce(address)),
		StorageHash:  storageRoot,
		StorageProof: storageProof,
	}, statedb.GetCode(address), statedb.Error()
}

// proofList implements ethdb.KeyValueWriter and collects the proofs as
// hex-strings for delivery to rpc-caller.
type proofList []string

func (n *proofList) Put(key []byte, value []byte) error {
	*n = append(*n, hexutil.Encode(value))
	return nil
}

func (n *proofList) Delete(key []byte) error {
	panic("not supported")
}
