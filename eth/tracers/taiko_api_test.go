package tracers

import (
	"context"
	"math/big"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rpc"
)

func (b *testBackend) BlockChain() *core.BlockChain {
	return b.chain
}

func TestProvingPreflights(t *testing.T) {
	// Initialize test accounts
	accounts := newAccounts(3)
	genesis := &core.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
			accounts[0].addr: {Balance: big.NewInt(params.Ether)},
			accounts[1].addr: {Balance: big.NewInt(params.Ether)},
			accounts[2].addr: {Balance: big.NewInt(params.Ether)},
		},
	}
	genBlocks := 50
	signer := types.HomesteadSigner{}

	var (
		ref   atomic.Uint32 // total refs has made
		rel   atomic.Uint32 // total rels has made
		nonce uint64
	)
	backend := newTestBackend(t, genBlocks, genesis, func(i int, b *core.BlockGen) {
		// Transfer from account[0] to account[1]
		//    value: 1000 wei
		//    fee:   0 wei
		for j := 0; j < i+1; j++ {
			tx, _ := types.SignTx(types.NewTransaction(nonce, accounts[1].addr, big.NewInt(1000), params.TxGas, b.BaseFee(), nil), signer, accounts[0].key)
			b.AddTx(tx)
			nonce += 1
		}
	})
	backend.refHook = func() { ref.Add(1) }
	backend.relHook = func() { rel.Add(1) }
	api := NewAPI(backend)

	// single := `{"txHash":"0x0000000000000000000000000000000000000000000000000000000000000000","result":{"gas":21000,"failed":false,"returnValue":"","structLogs":[]}}`
	var cases = []struct {
		start  uint64
		end    uint64
		config *TraceConfig
	}{
		{0, 50, nil},  // the entire chain range, blocks [1, 50]
		{10, 20, nil}, // the middle chain range, blocks [11, 20]
	}
	for _, c := range cases {
		ref.Store(0)
		rel.Store(0)

		from, _ := api.blockByNumber(context.Background(), rpc.BlockNumber(c.start))
		to, _ := api.blockByNumber(context.Background(), rpc.BlockNumber(c.end))
		resCh := api.provingPreflights(context.Background(), from, to, c.config)

		next := c.start + 1
		for result := range resCh {
			if have, want := result.Block.NumberU64(), next; have != want {
				t.Fatalf("unexpected tracing block, have %d want %d", have, want)
			}
			if next == 1 {
				if have, want := len(result.InitAccountProofs), 2; have != want {
					t.Fatalf("unexpected result length, have %d want %d", have, want)
				}
			} else {
				if have, want := len(result.InitAccountProofs), 3; have != want {
					t.Fatalf("unexpected result length, have %d want %d", have, want)
				}
			}
			next += 1
		}
		if next != c.end+1 {
			t.Error("Missing tracing block")
		}

		if nref, nrel := ref.Load(), rel.Load(); nref != nrel {
			t.Errorf("Ref and deref actions are not equal, ref %d rel %d", nref, nrel)
		}
	}
}
