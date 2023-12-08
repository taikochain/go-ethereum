package taiko_genesis

import (
	_ "embed"
)

//go:embed internal_l2a.json
var InternalL2AGenesisAllocJSON []byte

//go:embed internal_l2b.json
var InternalL2BGenesisAllocJSON []byte

//go:embed snaefellsjokull.json
var SnaefellsjokullGenesisAllocJSON []byte

//go:embed askja.json
var AskjaGenesisAllocJSON []byte

//go:embed grimsvotn.json
var GrimsvotnGenesisAllocJSON []byte

//go:embed eldfell.json
var EldfellGenesisAllocJSON []byte

//go:embed jolnir.json
var JolnirGenesisAllocJSON []byte
