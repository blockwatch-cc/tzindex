package rpc

import (
	"blockwatch.cc/tzgo/tezos"
	"testing"
)

func TestParams(t *testing.T) {
	var (
		lastProto  tezos.ProtocolHash
		deployment int
	)

	// genesis params
	p := NewParams().WithChainId(Mainnet).WithProtocol(ProtoGenesis).WithDeployment(-1)
	p.StartHeight = 0
	p.StartCycle = 0

	// walk test blocks
	for _, v := range paramBlocks {
		// update test state
		isProtoUpgrade := !lastProto.Equal(v.Protocol)
		if isProtoUpgrade {
			deployment++
			lastProto = v.Protocol
		}

		// prepare block
		block := Block{
			Protocol: v.Protocol,
			ChainId:  Mainnet,
			Header: BlockHeader{
				Level: v.LevelInfo.Level,
				Proto: deployment,
			},
			Metadata: v,
		}
		height, cycle := block.GetLevel(), block.GetCycle()

		// prepare params
		next := protoConstants[v.Protocol].Params().
			WithChainId(Mainnet).
			WithProtocol(block.Protocol).
			WithDeployment(block.Header.Proto)
		if height <= 1 {
			next.Deployment--
		}
		if p.Version < next.Version {
			next.StartHeight = block.GetLevel()
			next.StartCycle = block.GetCycle()
			next.StartOffset = block.GetCyclePosition()
			switch next.Version {
			// v009 and v010 start one block too early due to a bug in v008
			case 9:
				next.StartOffset = 4095 // will add +1 to protocol start level
			case 10:
				next.StartOffset = -1 // shift into correct cycle
				next.StartCycle++
			}
		} else {
			next.StartHeight = p.StartHeight
			next.StartCycle = p.StartCycle
			next.StartOffset = p.StartOffset
		}
		p = next

		check := paramResults[height]

		// test param functions
		if !p.ContainsHeight(height) {
			t.Errorf("v%03d params.ContainsHeight(%d) failed", p.Version, height)
		}
		if !p.ContainsCycle(cycle) {
			t.Errorf("v%03d %d params.ContainsCycle(%d) failed", p.Version, height, cycle)
		}
		if have, want := p.IsCycleStart(height), check.IsCycleStart(); have != want {
			t.Errorf("v%03d params.IsCycleStart(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := p.IsCycleEnd(height), check.IsCycleEnd(); have != want {
			t.Errorf("v%03d params.IsCycleEnd(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := p.HeightToCycle(height), check.Cycle; have != want {
			t.Errorf("v%03d params.HeightToCycle(%d) mismatch: have=%d want=%d", p.Version, height, have, want)
		}
		cstart := p.CycleStartHeight(cycle)
		cend := p.CycleEndHeight(cycle)
		cpos := p.CyclePosition(height)
		if cstart < 0 {
			t.Errorf("v%03d %d negative cycle start %d", p.Version, height, cstart)
		}
		if cend < 0 {
			t.Errorf("v%03d %d negative cycle end %d", p.Version, height, cend)
		}
		if cpos < 0 {
			t.Errorf("v%03d %d negative cycle pos %d", p.Version, height, cpos)
		}
		if cstart >= cend {
			t.Errorf("v%03d %d cycle start %d > end %d", p.Version, height, cstart, cend)
		}
		if cstart+cpos != height {
			t.Errorf("v%03d %d cycle pos %d + start %d != height", p.Version, height, cstart, cpos)
		}

		// test bundle functions
		b := Bundle{
			Block:  &block,
			Params: p,
		}
		if have, want := b.IsCycleStart(), check.IsCycleStart(); have != want {
			t.Errorf("v%03d bundle.IsCycleStart(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := b.IsCycleEnd(), check.IsCycleEnd(); have != want {
			t.Errorf("v%03d bundle.IsCycleEnd(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := b.IsSnapshotBlock(), check.IsSnapshot(); have != want {
			t.Errorf("v%03d bundle.IsSnapshotBlock(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := b.IsVoteStart(), check.IsVoteStart(); have != want {
			t.Errorf("v%03d bundle.IsVoteStart(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := b.IsVoteEnd(), check.IsVoteEnd(); have != want {
			t.Errorf("v%03d bundle.IsVoteEnd(%d) mismatch: have=%t want=%t", p.Version, height, have, want)
		}
		if have, want := b.GetSnapshotIndex(), check.Snap; have != want {
			t.Errorf("v%03d bundle.GetSnapshotIndex(%d) mismatch: have=%d want=%d", p.Version, height, have, want)
		}
	}
}

type paramResult struct {
	Cycle int64
	Snap  int
	Flags byte // 16 Snapshot | 8 CycleStart | 4 CycleEnd | 2 VoteStart | 1 VoteEnd
}

func (p paramResult) IsSnapshot() bool {
	return (p.Flags>>4)&0x1 > 0
}

func (p paramResult) IsCycleStart() bool {
	return (p.Flags>>3)&0x1 > 0
}

func (p paramResult) IsCycleEnd() bool {
	return (p.Flags>>2)&0x1 > 0
}

func (p paramResult) IsVoteStart() bool {
	return (p.Flags>>1)&0x1 > 0
}

func (p paramResult) IsVoteEnd() bool {
	return p.Flags&0x1 > 0
}

var paramResults = map[int64]paramResult{
	0:       {0, -1, 0},            // genesis
	1:       {0, -1, 8},            // bootstrap
	2:       {0, -1, 2},            // v001 start
	28082:   {6, 12, 0},            // ---> end
	28083:   {6, 12, 0},            // v002 start
	204761:  {49, 14, 0},           // ---> end
	204762:  {49, 14, 0},           // v003 start
	458752:  {111, 15, 16 + 4 + 1}, // ---> end
	458753:  {112, -1, 8 + 2},      // v004 start
	655360:  {159, 15, 16 + 4 + 1}, // ---> end
	655361:  {160, -1, 8 + 2},      // v005 start
	851968:  {207, 15, 16 + 4 + 1}, // ---> end
	851969:  {208, -1, 8 + 2},      // v006 start
	1212416: {295, 15, 16 + 4 + 1}, // ---> end
	1212417: {296, -1, 8 + 2},      // v007 start
	1343488: {327, 15, 16 + 4 + 1}, // ---> end
	1343489: {328, -1, 8 + 2},      // v008 start Edo Bug
	1466367: {357, 14, 1},          // ---> end (proto end, vote end, !cycle end)
	1466368: {357, 15, 16 + 4 + 2}, // v009 start (proto start, vote start, cycle end)
	1466369: {358, -1, 8},          // v009 cycle start
	1589247: {387, 14, 1},          // --> end (proto end, vote end, !cycle end)
	1589248: {387, 15, 16 + 4 + 2}, // v010 start (proto start, vote start, cycle end)
	1589249: {388, -1, 8},          // v010 cycle start
	1916928: {427, 15, 16 + 4 + 1}, // --> end
	1916929: {428, -1, 8 + 2},      // v011 start
	2244608: {467, 15, 16 + 4 + 1}, // --> end
	2244609: {468, -1, 8 + 2},      // v012 start
	2490368: {497, 15, 16 + 4 + 1}, // --> end
	2490369: {498, -1, 8 + 2},      // v013 start
	2736128: {527, 15, 16 + 4 + 1}, // --> end
	2736129: {528, -1, 8 + 2},      // v014 start
	2981888: {557, 15, 16 + 4 + 1}, // --> end
	2981889: {558, -1, 8 + 2},      // v015 start
	3268608: {592, 15, 16 + 4 + 1}, // --> end
	3268609: {593, -1, 8 + 2},      // v016 start
}

var paramBlocks = []BlockMetadata{
	{
		// genesis
		Protocol:         ProtoGenesis,
		NextProtocol:     ProtoBootstrap,
		LevelInfo:        &LevelInfo{},
		VotingPeriodInfo: &VotingPeriodInfo{},
	}, {
		// bootstrap
		Protocol:     ProtoBootstrap,
		NextProtocol: ProtoV001,
		LevelInfo: &LevelInfo{
			Level: 1,
		},
		VotingPeriodInfo: &VotingPeriodInfo{},
	}, {
		// v1 start
		Protocol:     ProtoV001,
		NextProtocol: ProtoV001,
		LevelInfo: &LevelInfo{
			Level:              2,
			Cycle:              0,
			CyclePosition:      1,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  1,
			Remaining: 32766,
		},
	}, {
		// v1 end
		Protocol:     ProtoV001,
		NextProtocol: ProtoV002,
		LevelInfo: &LevelInfo{
			Level:              28082,
			Cycle:              6,
			CyclePosition:      3505,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  28081,
			Remaining: 4686,
		},
	}, {
		// v2 start
		Protocol:     ProtoV002,
		NextProtocol: ProtoV002,
		LevelInfo: &LevelInfo{
			Level:              28083,
			Cycle:              6,
			CyclePosition:      3506,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  28082,
			Remaining: 4685,
		},
	}, {
		// v2 end
		Protocol:     ProtoV002,
		NextProtocol: ProtoV003,
		LevelInfo: &LevelInfo{
			Level:              204761,
			Cycle:              49,
			CyclePosition:      4056,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  8152,
			Remaining: 24615,
		},
	}, {
		// v3 start
		Protocol:     ProtoV003,
		NextProtocol: ProtoV003,
		LevelInfo: &LevelInfo{
			Level:              204762,
			Cycle:              49,
			CyclePosition:      4057,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  8153,
			Remaining: 24614,
		},
	}, {
		// v3 end
		Protocol:     ProtoV003,
		NextProtocol: ProtoV004,
		LevelInfo: &LevelInfo{
			Level:              458752,
			Cycle:              111,
			CyclePosition:      4095,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  32767,
			Remaining: 0,
		},
	}, {
		// v4 start
		Protocol:     ProtoV004,
		NextProtocol: ProtoV004,
		LevelInfo: &LevelInfo{
			Level:              458753,
			Cycle:              112,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 32767,
		},
	}, {
		// v4 end
		Protocol:     ProtoV004,
		NextProtocol: PsBabyM1,
		LevelInfo: &LevelInfo{
			Level:              655360,
			Cycle:              159,
			CyclePosition:      4095,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  32767,
			Remaining: 0,
		},
	}, {
		// v5 start
		Protocol:     PsBabyM1,
		NextProtocol: PsBabyM1,
		LevelInfo: &LevelInfo{
			Level:              655361,
			Cycle:              160,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 32767,
		},
	}, {
		// v5 end
		Protocol:     PsBabyM1,
		NextProtocol: PsCARTHA,
		LevelInfo: &LevelInfo{
			Level:              851968,
			Cycle:              207,
			CyclePosition:      4095,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  32767,
			Remaining: 0,
		},
	}, {
		// v6 start
		Protocol:     PsCARTHA,
		NextProtocol: PsCARTHA,
		LevelInfo: &LevelInfo{
			Level:              851969,
			Cycle:              208,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 32767,
		},
	}, {
		// v6 end
		Protocol:     PsCARTHA,
		NextProtocol: PsDELPH1,
		LevelInfo: &LevelInfo{
			Level:              1212416,
			Cycle:              295,
			CyclePosition:      4095,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  32767,
			Remaining: 0,
		},
	}, {
		// v7 start
		Protocol:     PsDELPH1,
		NextProtocol: PsDELPH1,
		LevelInfo: &LevelInfo{
			Level:              1212417,
			Cycle:              296,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 32767,
		},
	}, {
		// v7 end
		Protocol:     PsDELPH1,
		NextProtocol: PtEdo2Zk,
		LevelInfo: &LevelInfo{
			Level:              1343488,
			Cycle:              327,
			CyclePosition:      4095,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  32767,
			Remaining: 0,
		},
	}, {
		// v8 start
		Protocol:     PtEdo2Zk,
		NextProtocol: PtEdo2Zk,
		LevelInfo: &LevelInfo{
			Level:              1343489,
			Cycle:              328,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  1, // Edo bug
			Remaining: 20478,
		},
	}, {
		// v8 end
		Protocol:     PtEdo2Zk,
		NextProtocol: PsFLoren,
		LevelInfo: &LevelInfo{
			Level:              1466367,
			Cycle:              357,
			CyclePosition:      4094,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  20479,
			Remaining: 0,
		},
	}, {
		// v9 start
		Protocol:     PsFLoren,
		NextProtocol: PsFLoren,
		LevelInfo: &LevelInfo{
			Level:              1466368,
			Cycle:              357,
			CyclePosition:      4095,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 20479,
		},
	}, {
		// v9 first cycle block
		Protocol:     PsFLoren,
		NextProtocol: PsFLoren,
		LevelInfo: &LevelInfo{
			Level:              1466369,
			Cycle:              358,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  1, // Edo bug
			Remaining: 20478,
		},
	}, {
		// v9 end
		Protocol:     PsFLoren,
		NextProtocol: PtGRANAD,
		LevelInfo: &LevelInfo{
			Level:              1589247,
			Cycle:              387,
			CyclePosition:      4094,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  20479,
			Remaining: 0,
		},
	}, {
		// v10 start
		Protocol:     PtGRANAD,
		NextProtocol: PtGRANAD,
		LevelInfo: &LevelInfo{
			Level:              1589248,
			Cycle:              387,
			CyclePosition:      4095,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v10 first cycle block
		Protocol:     PtGRANAD,
		NextProtocol: PtGRANAD,
		LevelInfo: &LevelInfo{
			Level:              1589249,
			Cycle:              388,
			CyclePosition:      0,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 40959,
		},
	}, {
		// v10 end
		Protocol:     PtGRANAD,
		NextProtocol: PtHangz2,
		LevelInfo: &LevelInfo{
			Level:              1916928,
			Cycle:              427,
			CyclePosition:      8191,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v11 start
		Protocol:     PtHangz2,
		NextProtocol: PtHangz2,
		LevelInfo: &LevelInfo{
			Level:              1916929,
			Cycle:              428,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 40959,
		},
	}, {
		// v11 end
		Protocol:     PtHangz2,
		NextProtocol: Psithaca,
		LevelInfo: &LevelInfo{
			Level:              2244608,
			Cycle:              467,
			CyclePosition:      8191,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v12 start
		Protocol:     Psithaca,
		NextProtocol: Psithaca,
		LevelInfo: &LevelInfo{
			Level:              2244609,
			Cycle:              468,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 40959,
		},
	}, {
		// v12 end
		Protocol:     Psithaca,
		NextProtocol: PtJakart,
		LevelInfo: &LevelInfo{
			Level:              2490368,
			Cycle:              497,
			CyclePosition:      8191,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v13 start
		Protocol:     PtJakart,
		NextProtocol: PtJakart,
		LevelInfo: &LevelInfo{
			Level:              2490369,
			Cycle:              498,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 40959,
		},
	}, {
		// v13 end
		Protocol:     PtJakart,
		NextProtocol: PtKathma,
		LevelInfo: &LevelInfo{
			Level:              2736128,
			Cycle:              527,
			CyclePosition:      8191,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v14 start
		Protocol:     PtKathma,
		NextProtocol: PtKathma,
		LevelInfo: &LevelInfo{
			Level:              2736129,
			Cycle:              528,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 40959,
		},
	}, {
		// v14 end
		Protocol:     PtKathma,
		NextProtocol: PtLimaPt,
		LevelInfo: &LevelInfo{
			Level:              2981888,
			Cycle:              557,
			CyclePosition:      8191,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v15 start
		Protocol:     PtLimaPt,
		NextProtocol: PtLimaPt,
		LevelInfo: &LevelInfo{
			Level:              2981889,
			Cycle:              558,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 40959,
		},
	}, {
		// v15 end
		Protocol:     PtLimaPt,
		NextProtocol: PtMumbai,
		LevelInfo: &LevelInfo{
			Level:              3268608,
			Cycle:              592,
			CyclePosition:      8191,
			ExpectedCommitment: true,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  40959,
			Remaining: 0,
		},
	}, {
		// v16 start
		Protocol:     PtMumbai,
		NextProtocol: PtMumbai,
		LevelInfo: &LevelInfo{
			Level:              3268609,
			Cycle:              593,
			CyclePosition:      0,
			ExpectedCommitment: false,
		},
		VotingPeriodInfo: &VotingPeriodInfo{
			Position:  0,
			Remaining: 81912,
		},
	},
}

var (
	startParams = Constants{
		PreservedCycles:        5,
		BlocksPerCycle:         4096,
		BlocksPerCommitment:    32,
		BlocksPerStakeSnapshot: 256,
		BlocksPerVotingPeriod:  32768,
	}
	edoParams = Constants{
		PreservedCycles:        5,
		BlocksPerCycle:         4096,
		BlocksPerCommitment:    32,
		BlocksPerStakeSnapshot: 256,
		BlocksPerVotingPeriod:  20478, // !!
	}
	granadaParams = Constants{
		PreservedCycles:        5,
		BlocksPerCycle:         8192,  // !!
		BlocksPerCommitment:    64,    // !!
		BlocksPerStakeSnapshot: 512,   // !!
		BlocksPerVotingPeriod:  40956, // !!
	}
	mumbaiParams = Constants{
		PreservedCycles:        5,
		BlocksPerCycle:         16384, // !!
		BlocksPerCommitment:    128,   // !!
		BlocksPerStakeSnapshot: 1024,  // !!
		BlocksPerVotingPeriod:  81912, // !!
	}

	protoConstants = map[tezos.ProtocolHash]Constants{
		ProtoGenesis:   startParams,
		ProtoBootstrap: startParams,
		ProtoV001:      startParams,
		ProtoV002:      startParams,
		ProtoV003:      startParams,
		PtAthens:       startParams,
		PsBabyM1:       startParams,
		PsCARTHA:       startParams,
		PsDELPH1:       startParams,
		PtEdo2Zk:       edoParams,
		PsFLoren:       edoParams,
		PtGRANAD:       granadaParams,
		PtHangz2:       granadaParams,
		Psithaca:       granadaParams,
		PtJakart:       granadaParams,
		PtKathma:       granadaParams,
		PtLimaPt:       granadaParams,
		PtMumbai:       mumbaiParams,
	}
)
