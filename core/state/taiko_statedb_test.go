package state

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

// TestCopy tests that copying a StateDB object indeed makes the original and
// the copy independent of each other. This test is a regression test against
// https://github.com/ethereum/go-ethereum/pull/15549.
func TestTouchedAccounts(t *testing.T) {
	// Create a random state test to copy and modify "independently"
	orig, _ := New(types.EmptyRootHash, NewDatabaseForTesting())

	accountCounts := 10

	for i := byte(0); i < byte(accountCounts); i++ {
		obj := orig.getOrNewStateObject(common.BytesToAddress([]byte{i}))
		obj.AddBalance(uint256.NewInt(uint64(i)), tracing.BalanceChangeUnspecified)
		orig.updateStateObject(obj)
	}
	orig.Finalise(false)

	touchedAccs := orig.TouchedAccounts()
	if have, want := len(touchedAccs), accountCounts; have != want {
		t.Fatalf("have %d touched accounts, want %d", have, want)
	}

	// touch slots
	for i := byte(0); i < 3; i++ {
		orig.getOrNewStateObject(common.BytesToAddress([]byte{i})).SetState(common.Hash{i}, common.Hash{i})
	}
	orig.Finalise(false)
	touchedAccs = orig.TouchedAccounts()
	for i := byte(0); i < 3; i++ {
		if have, want := len(touchedAccs[common.BytesToAddress([]byte{i})]), 1; have != want {
			t.Fatalf("have %d touched accounts, want %d", have, want)
		}
	}

	// destroy accounts
	for i := byte(0); i < 3; i++ {
		orig.SelfDestruct(common.BytesToAddress([]byte{i}))
	}
	orig.Finalise(false)

	touchedAccs = orig.TouchedAccounts()
	if have, want := len(touchedAccs), accountCounts; have != want {
		t.Fatalf("have %d touched accounts, want %d", have, want)
	}

	// create empty accounts
	for i := byte(10); i < 12; i++ {
		orig.getOrNewStateObject(common.BytesToAddress([]byte{i}))
	}
	orig.Finalise(true)

	touchedAccs = orig.TouchedAccounts()
	if have, want := len(touchedAccs), accountCounts; have != want {
		t.Fatalf("have %d touched accounts, want %d", have, want)
	}
}
