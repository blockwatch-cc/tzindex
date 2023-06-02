// Copyright (c) 2020-2023 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"blockwatch.cc/tzgo/tezos"
)

var (
	ParseProto = tezos.MustParseProtocolHash
	ParseChain = tezos.MustParseChainIdHash
)

var (
	ProtoAlpha     = ParseProto("ProtoALphaALphaALphaALphaALphaALphaALphaALphaDdp3zK")
	ProtoGenesis   = ParseProto("PrihK96nBAFSxVL1GLJTVhu9YnzkMFiBeuJRPA8NwuZVZCE1L6i")
	ProtoBootstrap = ParseProto("Ps9mPmXaRzmzk35gbAYNCAw6UXdE2qoABTHbN2oEEc1qM7CwT9P")
	ProtoV001      = ParseProto("PtCJ7pwoxe8JasnHY8YonnLYjcVHmhiARPJvqcC6VfHT5s8k8sY")
	ProtoV002      = ParseProto("PsYLVpVvgbLhAhoqAkMFUo6gudkJ9weNXhUYCiLDzcUpFpkk8Wt")
	ProtoV003      = ParseProto("PsddFKi32cMJ2qPjf43Qv5GDWLDPZb3T3bF6fLKiF5HtvHNU7aP")
	ProtoV004      = ParseProto("Pt24m4xiPbLDhVgVfABUjirbmda3yohdN82Sp9FeuAXJ4eV9otd")
	ProtoV005_2    = ParseProto("PsBabyM1eUXZseaJdmXFApDSBqj8YBfwELoxZHHW77EMcAbbwAS")
	ProtoV006_2    = ParseProto("PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb")
	ProtoV007      = ParseProto("PsDELPH1Kxsxt8f9eWbxQeRxkjfbxoqM52jvs5Y5fBxWWh4ifpo")
	ProtoV008_2    = ParseProto("PtEdo2ZkT9oKpimTah6x2embF25oss54njMuPzkJTEi5RqfdZFA")
	ProtoV009      = ParseProto("PsFLorenaUUuikDWvMDr6fGBRG8kt3e3D3fHoXK1j1BFRxeSH4i")
	ProtoV010      = ParseProto("PtGRANADsDU8R9daYKAgWnQYAJ64omN1o3KMGVCykShA97vQbvV")
	ProtoV011_2    = ParseProto("PtHangz2aRngywmSRGGvrcTyMbbdpWdpFKuS4uMWxg2RaH9i1qx")
	ProtoV012_2    = ParseProto("Psithaca2MLRFYargivpo7YvUr7wUDqyxrdhC5CQq78mRvimz6A")
	ProtoV013_2    = ParseProto("PtJakart2xVj7pYXJBXrqHgd82rdkLey5ZeeGwDgPp9rhQUbSqY")
	ProtoV014      = ParseProto("PtKathmankSpLLDALzWw7CGD2j2MtyveTwboEYokqUCP4a1LxMg")
	ProtoV015      = ParseProto("PtLimaPtLMwfNinJi9rCfDPWea8dFgTZ1MeJ9f1m2SRic6ayiwW")
	ProtoV016      = ParseProto("PtMumbaiiFFEGbew1rRjzSPyzRbA51Tm3RVZL5suHPxSZYDhCEc")
	ProtoV016_2    = ParseProto("PtMumbai2TmsJHNGRkD8v8YDbtao7BLUC3wjASn1inAKLFCjaH1")
	ProtoV017      = ParseProto("PtNairobiyssHuh87hEhfVBGCVrK3WnS8Z2FT4ymB5tAa4r1nQf")

	// aliases
	PtAthens  = ProtoV004
	PsBabyM1  = ProtoV005_2
	PsCARTHA  = ProtoV006_2
	PsDELPH1  = ProtoV007
	PtEdo2Zk  = ProtoV008_2
	PsFLoren  = ProtoV009
	PtGRANAD  = ProtoV010
	PtHangz2  = ProtoV011_2
	Psithaca  = ProtoV012_2
	PtJakart  = ProtoV013_2
	PtKathma  = ProtoV014
	PtLimaPt  = ProtoV015
	PtMumbai  = ProtoV016_2
	PtNairobi = ProtoV017

	Mainnet      = ParseChain("NetXdQprcVkpaWU")
	Ghostnet     = ParseChain("NetXnHfVqm9iesp")
	Jakartanet   = ParseChain("NetXLH1uAxK7CCh")
	Kathmandunet = ParseChain("NetXi2ZagzEsXbZ")
	Limanet      = ParseChain("NetXizpkH94bocH")
	Mumbainet    = ParseChain("NetXgbcrNtXD2yA")
	Nairobinet   = ParseChain("NetXyuzvDo2Ugzb")

	Versions = map[tezos.ProtocolHash]int{
		ProtoGenesis:   0,
		ProtoBootstrap: 0,
		ProtoV001:      1,
		ProtoV002:      2,
		ProtoV003:      3,
		ProtoV004:      4,
		ProtoV005_2:    5,
		ProtoV006_2:    6,
		ProtoV007:      7,
		ProtoV008_2:    8,
		ProtoV009:      9,
		ProtoV010:      10,
		ProtoV011_2:    11,
		ProtoV012_2:    12,
		ProtoV013_2:    13,
		ProtoV014:      14,
		ProtoV015:      15,
		ProtoV016:      16,
		ProtoV016_2:    16,
		ProtoV017:      17,
		ProtoAlpha:     18,
	}
)
