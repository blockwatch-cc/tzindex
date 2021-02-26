// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package chain

var (
	ProtoV000   = ParseProtocolHashSafe("Ps9mPmXaRzmzk35gbAYNCAw6UXdE2qoABTHbN2oEEc1qM7CwT9P")
	ProtoV001   = ParseProtocolHashSafe("PtCJ7pwoxe8JasnHY8YonnLYjcVHmhiARPJvqcC6VfHT5s8k8sY")
	ProtoV002   = ParseProtocolHashSafe("PsYLVpVvgbLhAhoqAkMFUo6gudkJ9weNXhUYCiLDzcUpFpkk8Wt")
	ProtoV003   = ParseProtocolHashSafe("PsddFKi32cMJ2qPjf43Qv5GDWLDPZb3T3bF6fLKiF5HtvHNU7aP")
	ProtoV004   = ParseProtocolHashSafe("Pt24m4xiPbLDhVgVfABUjirbmda3yohdN82Sp9FeuAXJ4eV9otd")
	ProtoV005_1 = ParseProtocolHashSafe("PsBABY5HQTSkA4297zNHfsZNKtxULfL18y95qb3m53QJiXGmrbU")
	ProtoV005_2 = ParseProtocolHashSafe("PsBabyM1eUXZseaJdmXFApDSBqj8YBfwELoxZHHW77EMcAbbwAS")
	ProtoV006_1 = ParseProtocolHashSafe("PtCarthavAMoXqbjBPVgDCRd5LgT7qqKWUPXnYii3xCaHRBMfHH")
	ProtoV006_2 = ParseProtocolHashSafe("PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb")
	ProtoV007   = ParseProtocolHashSafe("PsDELPH1Kxsxt8f9eWbxQeRxkjfbxoqM52jvs5Y5fBxWWh4ifpo")
	ProtoV008_1 = ParseProtocolHashSafe("PtEdoTezd3RHSC31mpxxo1npxFjoWWcFgQtxapi51Z8TLu6v6Uq")
	ProtoV008_2 = ParseProtocolHashSafe("PtEdo2ZkT9oKpimTah6x2embF25oss54njMuPzkJTEi5RqfdZFA")

	Mainnet     = MustParseChainIdHash("NetXdQprcVkpaWU")
	Alphanet    = MustParseChainIdHash("NetXgtSLGNJvNye")
	Zeronet     = MustParseChainIdHash("NetXKakFj1A7ouL")
	Babylonnet  = MustParseChainIdHash("NetXUdfLh6Gm88t")
	Carthagenet = MustParseChainIdHash("NetXjD3HPJJjmcd")
	Delphinet   = MustParseChainIdHash("NetXm8tYqnMWky1")
	Edonet      = MustParseChainIdHash("NetXSp4gfdanies")
	Edonet2     = MustParseChainIdHash("NetXSgo1ZT2DRUG")

	// maximum depth of branches for ops to be included on chain, also
	// defines max depth of a possible reorg and max block priorities
	MaxBranchDepth int64 = 64
)

func (p *Params) ForNetwork(net ChainIdHash) *Params {
	pp := &Params{}
	*pp = *p
	pp.ChainId = net
	switch true {
	case Mainnet.IsEqual(net):
		pp.Network = "Mainnet"
	case Alphanet.IsEqual(net):
		pp.Network = "Alphanet"
	case Zeronet.IsEqual(net):
		pp.Network = "Zeronet"
	case Babylonnet.IsEqual(net):
		pp.Network = "Babylonnet"
	case Carthagenet.IsEqual(net):
		pp.Network = "Carthagenet"
	case Delphinet.IsEqual(net):
		pp.Network = "Delphinet"
	case Edonet.IsEqual(net):
		pp.Network = "Edonet"
	case Edonet2.IsEqual(net):
		pp.Network = "Edonet2"
	default:
		pp.Network = "Sandbox"
	}
	return pp
}

func (p *Params) ForProtocol(proto ProtocolHash) *Params {
	pp := &Params{}
	*pp = *p
	pp.Protocol = proto
	pp.NumVotingPeriods = 4
	switch true {
	case ProtoV000.IsEqual(proto):
		pp.Version = 0
		pp.ReactivateByTx = true
		pp.HasOriginationBug = true
		pp.SilentSpendable = true
	case ProtoV001.IsEqual(proto):
		pp.Version = 1
		pp.ReactivateByTx = true
		pp.HasOriginationBug = true
		pp.SilentSpendable = true
	case ProtoV002.IsEqual(proto):
		pp.Version = 2
		pp.ReactivateByTx = true
		pp.SilentSpendable = true
	case ProtoV003.IsEqual(proto):
		pp.Version = 3
		pp.ReactivateByTx = true
		pp.SilentSpendable = true
	case ProtoV004.IsEqual(proto):
		pp.Version = 4
		pp.SilentSpendable = true
		pp.Invoices = map[string]int64{
			"tz1iSQEcaGpUn6EW5uAy3XhPiNg7BHMnRSXi": 100 * 1000000,
		}
	case ProtoV005_1.IsEqual(proto) || ProtoV005_2.IsEqual(proto):
		pp.Version = 5
		pp.Invoices = map[string]int64{
			"KT1DUfaMfTRZZkvZAYQT5b3byXnvqoAykc43": 500 * 1000000,
		}
		pp.OperationTagsVersion = 1
	case ProtoV006_1.IsEqual(proto) || ProtoV006_2.IsEqual(proto):
		pp.Version = 6
		pp.OperationTagsVersion = 1
		// no invoice
	case ProtoV007.IsEqual(proto):
		pp.Version = 7
		pp.OperationTagsVersion = 1
		// no invoice
	case ProtoV008_2.IsEqual(proto) || ProtoV008_1.IsEqual(proto):
		pp.Version = 8
		pp.OperationTagsVersion = 1
		pp.NumVotingPeriods = 5
		if Mainnet.IsEqual(p.ChainId) {
			pp.StartBlockOffset = 1343488
		}
		// no invoice
	}
	return pp
}
