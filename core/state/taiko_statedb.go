package state

import (
	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/exp/maps"
)

// TouchedAccounts represents the storage of an account at a specific point in time.
type TouchedAccounts map[common.Address][]common.Hash

// TouchedAccounts returns a map of all touched accounts and their storage.
// Incudes self-destructed accounts, loaded accounts and new accounts exclude empty account.
func (s *StateDB) TouchedAccounts() TouchedAccounts {
	touched := make(TouchedAccounts, len(s.stateObjects))
	for addr, obj := range s.stateObjects {
		touched[addr] = maps.Keys(obj.originStorage)
	}
	for addr, obj := range s.stateObjectsDestruct {
		// ignore empty account because it won't affect the state
		if obj.selfDestructed || !obj.empty() {
			touched[addr] = maps.Keys(obj.originStorage)
		}
	}
	return touched
}
