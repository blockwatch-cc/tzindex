# Changelog

### v18.0.7

* d55a36c | Fix op_id reference in tickets to use external id
* ff7b706 | Add ticket by account filter
* 3558010 | Unified ticket issuer field name
* cdcc80a | Fix ticket update query

### v18.0.6

* afbf26c | Fix staker counter
* a157ab2 | Fix address mem reuse
* da8800d | Decode bigmap events once
* 1c6c7c2 | Silence empty stake warning on genesis
* ebffc2a | Fix metadata download
* 9fcf51d | Fix task scheduler and client leaks
* 054bedd | Add ticket index
* df1eb74 | Change last validator block lookups to use rights
* dfc90dd | Fix unstaked supply

### v18.0.5

* f257004 | Fix oxford stake migration in light mode

### v18.0.4

* ffdd089 | Export deposits limit on API
* b79a16f | Use deposits limit for oxford delegation capacity
* b7837b3 | Fix double accounting of Oxford fee
* 15dc972 | Expose unstaked balance on baker model

### v18.0.3

* b66c476 | Add missing finalize unstake to spendable balance flow
* 4a33fb6 | Fix unknown key in oxford bigmap migration
* 4ee9b47 | Add docker health checks
* d1f5281 | Update dockerfiles

### v18.0.2

* 9d43c38 | Add drone ci config
* 47736fc | Fix linter warnings
* 0477298 | Embed set delegate params with API operation
* 1a50e19 | Fix oxford fee flows
* 745656f | Use float value for baking power
* 318c389 | Revert table stats type to packdb version
* f28b566 | Add pre-Oxford frozen deposits to own stake in API response
* 9d628ef | Change staking capacity calc to take spendable balance into account
* ca56a25 | Fix metadata update calls
* e53e50f | Fix calling supply handler
* bd7eb8d | Fix using decoded op query arg

### v18.0.1

- Fix revert env var prefix to `TZ`

### v18.0.0

Oxford Staking
- new stake operations
- new balance update handling
- new slashing behavior
- new staking stats for accounts, bakers, chain, supply, income, snapshot
- new issuance behavior (new constants and new RPC)

CLI breaking change
- new default `-db.max_storage_entry_size=131072` (was 0 before)

Table name changes (/tables API)
- old names are deprecated but still work, will be removed in a later release
- `bigmaps` -> `bigmap_types`
- `election` -> `gov_election`
- `proposal` -> `gov_proposal`
- `vote` -> `gov_vote`
- `ballot` -> `gov_ballot`
- `stake` -> `gov_stake`

Account model changes
- new `staked_balance`
- new `frozen_rewards`
- new `unstaked_balance`
- new `lost_stake`
- new `is_staked`
- renamed `frozen_bond` -> `frozen_rollup_bond`
- renamed `lost_bond` -> `lost_rollup_bond`

Baker model changes
- new `total_stake` (own and foreign slashable stake)
- new `baking_power` (stake plus delegations, capped)
- new `active_stakers` (number of foreign stakers)
- new `staking_edge` (baker fee for staking)
- new `staking_limit` (individual staking cap, lower than max protocol cap)
- new `delegation_capacity` (total delegation capacity until cap)
- new `staking_capacity` (total stake capacity until cap)
- new `is_over_delegated` (overflow is not counted into baking power)
- new `is_over_staked` (overflow counts towards delegations)
- renamed `active_stake` -> `own_stake`
- renamed `staking_share` -> `network_share`
- renamed `frozen_bond` -> `frozen_rollup_bond`
- renamed `lost_bond` -> `lost_rollup_bond`
- removed `staking_balance` (replaced by `total_stake`)
- removed `deposits_limit`
- removed `total_delegations`
- removed `is_full` (replaced by `is_over_delegated` and `is_over_staked`)

Block model changes
- new `ai_vote`
- new `ai_ema`
- renamed `lb_esc_vote` -> `lb_vote`
- renamed `lb_esc_ema` -> `lb_ema`

Chain model changes
- new `total_stakers`
- new `active_stakers`
- new `inactive_stakers`
- removed `rolls`
- renamed `rolls_owners` -> `eligible_bakers`

Supply model changes
- new `unstaking`      // anything in unfreeze but not withdrawn
- new `frozen_stake`        // all
- new `frozen_baker_stake`  // baker only
- new `frozen_staker_stake` // staker only
- changed `staking` to only include slashable stake
- changed `active_staking` to only include slashable stake
- changed `inactive_staking` to only include slashable stake
- renamed `burned_absence` -> `burned_offline`

Income model
- new `own_stake`
- new `n_stakers`
- removed `active_stake`
- removed `snapshot_rolls`
- removed `total_deposits`

Snapshot model
- new `own_stake`
- new `n_stakers`
- removed `active_stake`
- removed `snapshot_rolls`
- removed `total_deposits`

Op model
- new types: `stake`, `unstake`, `finalize_unstake`, `set_delegate_parameters`, `stake_slash`
- don't store Ithaca+ endorsements and pre-endorsements, saves 30GB (50%) storage

Flow model
- new kind: `stake`
- new types: `stake`, `unstake`, `finalize_unstake`, `set_delegate_parameters`

Gov Vote model
- removed `eligible_rolls`
- removed `quorum_rolls`
- removed `turnout_rolls`
- removed `yay_rolls`
- removed `nay_rolls`
- removed `pass_rolls`

Gov Proposal model
- removed `rolls`

Gov Ballot model
- removed `rolls`

Chain config model changes
- removed `delay_increment_per_round`

Cycle
- new `active_stakers`
- new `block_reward`
- new `block_bonus_per_slot`
- new `max_block_reward`
- new `endorsement_reward_per_slot`
- new `nonce_revelation_reward`
- new `vdf_revelation_reward`
- new `lb_subsidy`
- renamed `roll_owners` -> `eligible_bakers`
- renamed `working_bakers` -> `unique_bakers`
- removed `rolls`
- removed `working_endorsers`

Metadata
- restructured token metadata handling
- renamed `asset_id` to `token_id`
- moved `asset.symbol` and `asset.decimals` into `asset.tokens[]` array

Other changes

- re-fetch origination storage for correct store and bigmap init

* 200676e1 | Fix migration snapshot handling
* 0e620ce | Purge metadata cache on update
* 43b31dd | Disable storing endorsements
* 64d2c5a | Fix smart rollup flows
* 07d345a | Ignore api_key query arg on tables and series
* 84afaf8 | Track addresses in transfer tickets ops
* b72fad4 | Fix indexing broken rollup ops
* 57d536f | Drop mumbainet references
* 5da48c2 | Upgrade TzPro SDK
* 8469ff4 | proxy: reload params on watcher exit

### v17.0.1 (v016-2023-02-26)

* Track addresses in transfer tickets ops
* Upgrade TzPro SDK
* Fix indexing broken rollups

### v17.0.0 (v016-2023-02-26)

Nairobi release
- Add Nairobi hashes, calc max proto version when unknown
- Add smart rollup data changes for cement operation

Improvements
- Log every block when in sync
- Strip unused hashes and sigs from endorsements (big decoding speed up)
- Improve internal cache memory usage
- Add listing internal operations sent by a contract
- Add `total_ticket_transfers` to chain model
- Add new block field `n_tickets` to API
- Add new block series field `n_tickets` to API
- Add new chain series field `total_ticket_transfers` to API
- Rename `server` log system to `api` (this applies also to config options e.g. new: `log.api`)
- Change metadata payout list to array
- Update country codes
- Add `stalled` status to crawler
- Add contract cache, clear builder contract map at end of block

Fixes
- Fix testnet dictator may not be a baker
- Fix audit mode false positive errors for inactive bakers
- Fix using delegation volume from flows instead of previous balance

### v16.1.5 (v016-2023-02-26)

Mumbai Release

- NEW: add Mumbai operations wrapped into `rollup_origination` and `rollup_transaction`
    - `smart_rollup_add_messages`
    - `smart_rollup_cement`
    - `smart_rollup_originate`
    - `smart_rollup_execute_outbox_message`
    - `smart_rollup_publish`
    - `smart_rollup_recover_bond`
    - `smart_rollup_refute`
    - `smart_rollup_timeout`
- NEW: add `transfer_ticket` op as explict operation (previously this was part of the rollup package and wrapped inside `rollup_transaction`)
- NEW: op model added fields
  - `ticket_updates` on eligible transaction between tz accounts (previously ticket transfer was limited to contracts)
  - `staker`, `loser`, `winner` as participants in smart rollup refutation games
- NEW: add block model `NTickets`
- NEW: add chain model `TotalTicketTransfers`
- NEW: extended list of metadata alias categories
- NEW: detect and track protocol activations independently of hard-coded block heights

Fixes
- crash on broken call params
- micheline type compare for nested unions, this fixes detecting allocated bigmap types
- decoding bootstrap deposits on testnets
- exclude unknown fields from endorsement table query
- entrypoint detection, continue on data decode errors
- take delegation volume from flows instead of previous balance
- parsing block height
- micheline translate bugs
- some entrypoint param decoding
- nil response crash
- account n_tx_success column name
- gov vote index
- stake in post-Ithaca index 15 snapshots
- contract table lookup, limit to successful originations
- rollup call stats query
- handle storage burn on transfer ticket op
- use staking balance for gov snapshots
- handle internal results on rollup output message execution

Improvements
- List internal ops sent by a contract
- Raise default op limit to max
- Sanitize API requests to reject zero hashes and negative heights
- Add virtual start|end_time columns to income API
- Store height in rights and income tables
- Process internal transaction after ticket transfer
- Protect against high memory usage when loading from (zero) op hash
- Prevent indexer stop on unexpected rollup data
- Protect against unexpected rollup calls
- Use spendable balance for audit
- Skip internal tx when building traffic ranks
- Add more verbose init errors
- Add auto retries to RPC client
- List factory created contracts
- CLI: change log system config names
  - `log.blockchain` to `log.etl`
  - `log.database` to `log.db`
- Upgrade to Go 1.20 and TzGo to v1.16



### v16.0.0 - v16.1.4

(unreleased)

### untagged

- FIX: Add missing delegation flow to drain op
- CLI: let cli args have priority over config file

### v15.0.2 (v015-2022-12-06)

- FIX: correct accounts for drain delegate reverse flows
- FIX: dependencies

### v15.0.1 (v015-2022-12-06)

- FIX: apply effects of `drain_delegate` operation on account balances

### v15.0.0 (v015-2022-12-06)

Lima upgrade

- CLI: BREAKING changes in CLI args (see README.md)
  - replace Cobra due to lack of maintenance, benefit: reduces binary size and decreases dependencies
  - dropped CPU profiling args, use the built-in HTTP profiling endpoints `/debug/pprof/*` instead
  - renamed config and environment args for database from `TZ_DATABASE_*` to `TZ_DB_*` and logging from `TZ_LOGGING_*` to `TZ_LOG_*`
  - replaced `rpc.host` + `rpc.port` options with `rpc.url`
- op model
    - add Lima operations `drain_delegate` and `update_consensus_key`
    - drop `days_destroyed` without replacement
    - add `code_hash` field on contract calls to help filter by receiver contract type
    - add Lima `ticket_updates` on contract calls that mint, burn or transfer tickets
- block model
    - add Lima `proposer_consensus_key_id` and `proposer_consensus_key` fields
    - add Lima `baker_consensus_key_id` and `baker_consensus_key` fields
- account model (update statistics fields)
    - replace `n_ops` with `n_tx_success`
    - replace `n_ops_failed` with `n_tx_failed`
    - replace `n_tx` by `n_tx_out` and `n_tx_in`
    - drop `token_gen_min` without replacement
    - drop `token_gen_max` without replacement
    - drop `n_constants` without replacement
    - drop `n_origination` without replacement
    - drop `n_delegation` without replacement
    - add `total_fees_used` for total fees from tx where this account is receiver
    - hide `frozen_bond`, `lost_bond`, `is_activated`, `is_baker`, `is_contract` flags if unused
- contract model (update statistics fields)
    - replace `n_calls_success` by `n_calls_in`
    - add `n_calls_out`
    - add `total_fees_used` for all fees pay for calling this contract
- ticket models
    - add `ticket_update` table to track ticket updates
    - add `ticket_type` table to track ticket types (== ticketer, type and content)
- constants model
    - replace `tokens_per_roll` with `minimal_stake`
    - remove `time_between_blocks` (use `minimal_block_delay` and `delay_increment_per_round` instead)
    - remove `block_rewards_v6` and `endorsement_rewards_v6` without replacement
- balance model
    - drop `valid_until` field and always store most recent balance
- Micheline
    - add new opcodes Lima `D_LAMBDA_REC`, `I_LAMBDA_REC`
    - replace deprecated opcode `TICKET` with new opcode of same name (breaking!)
- ZMQ
    - extended op data with new fields `code_hash`, `events` and `ticket_updates`
- fixes
    - fix missing bigmap type annotations post Jakarta
    - fix detecting protocol invoices
    - fix use consistent field `volume` for all invoices
    - fix balance history start value when out of request window
    - fix better bigmap type matching to find annotated types
    - fix entrypoint detection
    - fix prevent indexing process to stop on transaction data analysis errors


### v14.0.0 (v014-2022-09-06)

Kathmandu upgrade

Model changes

- NEW event model
  - store events emitted from smart contract calls
- operation model
  - add `vdf_revelation` and `increase_paid_storage` operations
- account model
  - store all ghost accounts (unfunded wallets that appear in smart contract storage)
- chain model
  - add `ghost_accounts` counter

Other changes

- ETL: fix bake+unfreeze split from block-level balance updates (edge case mainnet Polychain baker)
- ETL: fix protocol invoice feature
- API: don't return deleted bigmaps
- API: strip unused info from on-chain constants
- API: fix bigmap metadata timestamps
- API: add `events` field to contract calls (off by default on lists, enable with `storage=1`)
- API: fix time series fill
- API: remove some exotic chain configuration constants from `/explorer/config`

### v13.0.1 (v013-2022-06-15)

- fix Jakarta protocol detection on network migration
- fix pre-Ithaca protocol stake usage in gov and snapshot
- fix marking burned rewards as unfrozen to fix supply accounting
- add missing API fields to bigmap and chain models
- fix script top-level constants expansion
- use staking balance instead of active stake for snapshot selection

### v13.0.0 (v013-2022-06-15)

Jakarta release

- support new (transaction) **optimistic rollup** operations
- re-add previously removed indexes for `rights`, `snapshots`, `flows`, `income` and `governance` data
- extend income, supply, chain, block, and operation models with new rollup fields
- replace hash indexes on operation, block and bigmap tables with bloom filters
- split storage and bigmap updates from operation into separate tables
- use lazy storage diff for bigmap indexing
- use active stake for governance counters in Ithaca+
- new Micheline opcodes
- new metadata schema for DEXs
- support filter by vote period in ballot and vote tables
- keep zero delegators in snapshot index
- fix nested entrypoint detection

Model changes

- supply model
  - new `active_stake`, `frozen_bonds` and `burned_rollup` (for penalties)
- chain model
  - new `total_ops_failed`, `total_rollup_calls` and `total_rollups`
- flow model
  - new balance category `bond`
  - new operations types `rollup_origination`, `rollup_transaction`, `rollup_reward`, `rollup_penalty`
- op model
  - new types `rollup_origination` and `rollup_transaction`
  - rollup op arguments are packed into `parameters`
  - new parameter fields `l2_address`, `method` and `arguments`
  - different rollup op types are represented as method names
  - new `is_rollup` flag
- block model
  - new counter `n_rollup_calls`
- account model
  - new balance type `frozen_bond` for current rollup bonds
  - new `lost_bond` for rollup slashes
- contract model
  - new `storage_burn` counter
- vote model
  - new `eligible_stake`, `quorum_stake`, `turnout_stake`, `yay_stake`, `nay_stake`, `pass_stake`
- baker model
  - new `active_stake`
- snapshot model
  - new `active_stake`
- income model
  - add `lost_accusation_fees`, `lost_accusation_rewards`, `lost_accusation_deposits`, `lost_seed_fees`, `lost_seed_rewards`,
  - renamed `total_bonds` to `total_deposits` to avoid name conflict with rollup bonds
  - `contribution_percent` and `avg_contribution_64` are now counted up until position in cycle, formerly this was end of cycle which skewed numbers for the current cycle
- tip model
  - removed `rolls` and `roll_owners`
- bigmap model
  - add `delete_height` field to signal when a bigmap was removed from storage
  - fix detection for dynamic allocated bigmaps

### v12.0.3 (v012-2022-03-25)

- ETL: Detect pruned metadata and fail safely
- ETL: Refactor crawler state handling for more robustness
- ETL: Keep bakers always in funded state
- API: Fix time-series mem-leak
- ETL: Fix contract table updates
- ETL: Fix counting pre-Ithaca endorsement slots/power
- ETL: Set last seen on endorsing ops
- API: Revert subtracting baker own balance from staking capacity

### v12.0.2 (v012-2022-03-25)

- DB: fixed OR condition early return with empty args
- API: Fix pre/post-Ithaca cycle selection
- API: Fix contract table by code/iface/storage hash query
- API: Add total contracts count to tip
- ETL: Fix counting block bonus
- ETL: Support preendorsement power

### v12.0.1 (v012-2022-03-25/light)

- API: Fix resource leak at desc query
- ETL: Fix activation op balance
- API: Fix block and chain table column names
- ETL: Fix packdb journal delete state

### v12.0.0 (API v012-2022-03-25/light)

Ithaca consensus upgrade with new operation types, new balance updates and new deposit/reward payment mechanics. We reorganized tables by removing unnecessary data and statistics to save on-disc storage and cache memory. As Ithaca changes many concepts, terminology and transaction types we took the opportunity to overhaul the entire API, adding new baker and balance tables. With Tezos growing beyond the staking/baking use case we also decided to drop several baker/consensus data tables that were expensive to maintain, but provided limit benefits to the larger Tezos dapp ecosystem.

**Refactoring**

- improved RPC performance, added embedded RPC lib which reads necessary data only
- updated balance update management to Ithaca
- new baker table with baker specific data, saves space in account table
- partitioned operations table (endorsements are no longer stored to save space)
- deprecated `rights`, `snapshots`, `flows`, `income` and `governance` data

**Breaking changes**

- removed some fields from operation, block and account models (see below)
- renamed operation types (see below)
- renamed `implicit` operations to `events` (i.e. protocol activity not explicitly available as under an op hash or not available in operation receipts at all)
- all occurences of `delegate` and `delegate_id` have been renamed to `baker` and `baker_id` for consistency and to avoid confusion with `delegator` fields
- operation `row_id` has been deprecated in favour of a more stable `id` value
- operation `op_n` is a unique event counter within each block (before, batch and internal operations shared the same op_n)
- penalty operations now use `accuser` and `offender` instead of `sender` and `receiver`
- internal operations now use `source` instead of `creator` for the outer transaction signer
- operations in lists (block, account) no longer contain storage updates, use `storage=1` to add
 - operating lists default to `order=desc` (most recent transactions first), use `order=asc` for previous behaviour)
- renamed `/explorer/baker` to `/explorer/bakers`

**Complete list of changes**

- API: op model
    - `/explorer/block/{hash}/operations` and `/explorer/account/{address}/operations` no longer contain storage and bigmap diffs per default, use `?storage=1` query argument to add
    - operating lists now default to `order=desc` (i.e. they show the most recent transactions first, use `order=asc` for previous behaviour)
    - switched `sender` and `creator` accounts on internal operations such that an internal call always lists the original signer of the outer operation as sender
    - refactored op list ids for protocol upgrade events (`-3`), block-level events like auto (un)freeze and rewards payments (`-2`) and block header implicit operations like liquidity baking (`-1`)
    - dropped op model fields `has_data`, `branch_height`, `branch_hash`, `branch_depth`, `is_orphan`, `is_sapling`, `entrypoint_id`, `gas_price`, `storage_size`, `has_data`
    - renamed fields
      - `is_implicit` to `is_event`
      - `block_hash` to `block`
      - `delegate_id` and `delegate` to `baker_id` and `baker`
    - renamed operation types
      - `seed_nonce_revelation` to `nonce_revelation`
      - `double_baking_evidence` to `double_baking`
      - `double_endorsement_evidence` to `double_endorsement`
    - added new Ithaca operations and event types for Tenderbake
      - `preendorsement` for preendorsements (only visible when block round > 0)
      - `double_preendorsement` for double signatures on preendorsements
      - `deposits_limit` for explicit baker deposit limit ops
      - `deposit` for explicit deposit freeze and unfreeze events
      - `reward` for endorsement reward payments
      - `bonus` for baker bonus payments (i.e. including more than threshold endorsements)
      - `subsidy` for minting liquidity baking subsidy (was type `transaction` before)
    - reordered table columns in table API output
- API: block model
  - we are no longer storing orphan blocks
  - dropped fields `endorsed_slots`, `fitness`, `is_orphan`, `parent_id`, `slot_mask`, `gas_price` and most counters such as `n_ops_contract`, `n_tx`, `n_activation`, `n_seed_nonce_revelation`, `n_double_baking_evidence`, `n_double_endorsement_evidence`, `n_endorsement`, `n_delegation`, `n_reveal`, `n_origination`, `n_proposal`, `n_ballot`, `n_register_constant`
  - added fields `proposer`, `proposer_id`, `minted_supply`
  - renamed fields
    - `priority` to `round`
    - `storage_size` to `storage_paid`
    - `n_ops` to `n_ops_applied`
    - `n_ops_implicit` to `n_events`
  - reordered table columns in table API output
- API: chain table
  - dropped field `total_paid_bytes` (duplicate of `total_storage_bytes`)
  - added fields `total_set_limits` and `total_preendorsements`
  - renamed fields `*_delegates` to `*_bakers`
  - renamed `total_double_baking_evidences` to `total_double_bakings`
  - renamed `total_double_endorsement_evidences` to `total_double_endorsements`
  - renamed `total_seed_nonce_revelations` to `total_nonce_revelations`
- API: moved field protocol `deployments` from `/explorer/tip` to separate endpoint `/explorer/protocols`
- API: account model
  - split account and baker data into two tables
  - dropped fields `blocks_endorsed`, `blocks_missed`, `blocks_stolen`, `slots_missed`
  - replaced `/explorer/account/{address}/managed` with `/explorer/account/{address}/contracts`
  - moved `/explorer/account/{address}/ballots` to `/explorer/bakers/{address}/votes`
- API: new baker model and `/explorer/bakers/{address}` endpoint for baker-specific data
  - `/votes` to list baker votes (defaults to descending order, i.e. newest first)
  - `/delegations` to list baker delegation events (defaults to descending order, i.e. newest first)
- API: new `balance` model to store historic account balances at end of each block, available as table and time-series API


### v11.0.0 (v011-2021-11-20)

Tezos Hangzhou protocol support.

- ETL: support for new Michelson smart contract on-chain **views**, **global constants** and **timelocks**
- ETL: new global constants table
- ETL: global constants are automatically injected into new originated contracts
- API: new endpoints `/tables/constant` and `/explorer/constant/{hash}`
- API: new typedefs for on-chain views in `/explorer/contracts/{address}/script`

Fixes and enhancements

- TZTOP: new live monitoring tool to gain insights into indexer query performance and memory usage
- ETL: deduplicate storage updates, reduces operation table size by 25%
- ETL: improve TTL block cache usage to reduces memory required for indexing
- ETL: improved burned supply accounting, count coins sent to burn address, count storage burn
- ETL: fix bigmap copy owner on origination
- ETL: update Tezos domains metadata model
- API: fix future vote end date
- API: disable bigmap ptr patching on origination storage
- ALIAS: support removal of empty metadata trees
- BUILD: Support windows build

Breaking changes
- CLI: `--light` mode has become default, for gov/income data run in `--full` mode
- API: block table field order has changed due to removal/addition of fields
- API: chain table field order has changed due to removal/addition of fields
- API: supply table field order has changed due to addition of fields

REST API

- API: new virtual argument `address` on operation table to filter for any occurence of an address across sender, receiver, creator and delegate fields
- API: improved historic bigmap cache build time by reusing existing state
- API: support long time-series collapse intervals `w` week, `M` month, `y` year
- API: block table (note: breaking changes to table field order)
  - removed `n_new_managed`, `n_new_implicit`
  - changed `n_new_accounts` to count EOA only (before this also counted new contracts)
  - new `n_register_constant`, `n_contract_calls`
- API: chain table and series (note: breaking changes to table field order)
  - removed `total_implicit`, `total_managed`
  - changed `total_accounts` to count EOA only (before this also counted new contracts)
  - new `total_constants`, `total_contract_calls`
  - new `dust_accounts` and `dust_delegators` for accounts with balance <1tez
- API: supply table and series (note: breaking changes to table field order)
  - renamed `burned_implicit` to `burned_allocation`
  - new `burned_storage` (storage fees) and `burned_explicit` (sent to burn address)
- API: deprecated `/system/mem` statistics and merged into `/system/tables`
- API: new `/system/sysstat` and `/system/apistat` endpoints
- API: moved internal management API endpoints
    - `/system/snapshot` -> `/system/tables/snapshot`
    - `/system/flush` -> `/system/tables/flush`
    - `/system/flush_journal` -> `/system/tables/flush_journal`
    - `/system/gc` -> `/system/tables/gc`
    - `/system/dump` -> `/system/tables/dump`
    - `/system/purge` -> `/system/caches/purge`
- API: calling `PUT /system/caches/purge` now purges table caches in addition to indexer caches


### 10.1.0

API changes

- API: op added `entrypoint` name
- API: op table added `block_hash` and `entrypoint` fields
- API: block table added `predecessor` block hash
- API: renamed op field `branch` to `branch_hash`

Bugfixes & Performance Improvements

- More sophisticated operation listing filter for delegations
- Support cache purging via API `PUT /system/purge`
- Skip sending discarded op hashes via ZMQ in light mode
- Skip sending non-stored ops via ZMQ in light mode
- Fix index queries for operation table
- Fix setting snapshot block marker at protocol upgrade
- Fix rights cache after Granada upgrade
- Proxy: fix consensus data fetch and queries

## 10.0.0 (unreleased)

Tezos Granada protocol support, 5x faster and 40% smaller bigmap index and new API features from API version `010-2021-07-24`. See our [complete API docu](https://tzstats.com/docs/api).

- ETL: new bigmap index split into 3 tables (allocs, updates, live keys)
- ETL: protocol migration: register subsidy contracts
- ETL: handle liquidity mining block subsidy
- ETL: added new flow type `subsidy` for liquidity mining flows
- ETL: fixed Edo governance offset bug
- ETL: disable index failure in failing checks in governance and income indexes
- ETL: disable transaction cancellation on shutdown to protect against database corruption

- API: replace single `bigmap` tables with 3 new tables `bigmaps` (allocs), `bigmap_updates` (updates only, not indexed) and `bigmap_values` (live key/value pairs, indexed by key hash)
- API: removed bigmap info field `is_removed` that used to signal whether a bigmap was deleted
- API: bigmap updates no longer contain alloc
- API: added new field `slots` to rights table, only used for endorsing rights to collect all slots assigned to a baker (before each individual slot was stored as a single rights table entry)
- API: replaced block field `endorsed_slots` (uint32) with `slot_mask` (hex string) on explorer and table endpoints, it remains a bitset (32 or 256 bits)
- API: added block fields `lb_esc_vote` and `lb_esc_ema` to track liquidity baking
- API: replaced block field `rights.slot` to `rights.slots` and changed type to array of integers to list all endorsement slots for a baker in a single entry
- API: op field `data` for type endorsement now contains a 256bit hex string after Granada instead of 32bit
- API: added field `minted_subsidy` to supply counters
- API: added blockchain config fields `liquidity_baking_escape_ema_threshold`, `liquidity_baking_subsidy`, `liquidity_baking_sunset_level`, `minimal_block_delay`

- ZMQ: changed block field `endorsed_slots` (now `slot_mask`) from uint32 to hex string
- ZMQ: added block fields `lb_esc_vote` and `lb_esc_ema` to track liquidity baking

- PROXY: extend rights cache by protocol version to keep versioned rights across protocol upgrades
- PROXY: add auto-upgrade procedure for rights bucket keys


## 9.1.0

Performance improvements and new API features from API version `009-2021-04-16`. See our [complete API docu](https://tzstats.com/docs/api).

- 3x better indexing time
- 5x improved runtime for certain queries
- 1.5-2x less storage and memory usage due to data pruning
- next generation smart contract data extraction engine

Updates and New Features

- DB: improved database read and write performance
- DB: improved database query interfaces and enabled logical OR in queries
- DB: fixed query bugs that could return non-matching results from journal
- DB: pruning of unused historic data in rights and snapshots
- DB: light mode no longer stores endorsement ops
- ETL: migrate to [TzGo](https://github.com/blockwatch-cc/tzgo) library
- API: JSON-schema validated metadata for accounts and tokens (use `meta=1` on operations and accounts/contracts to embed)
- CLI: new `tzalias` command to work with metadata

Breaking changes at a glance
- API: bigmap and storage type definitions and value layout changed
- API: block endorser list has been replaced by more detailed rights list (use `rights=1` to include)
- API: refactored contract data model

Fixes
- API: fixed election queries not returning all voting periods
- API: fixed collapse operation listings
- API: fixed invalid flags after bigmap rollback
- PROXY: fixed flushing block cache after reorg

For details please consult the API changelog for versions [2021-04-16](https://tzstats.com/docs/api#2021-04-16) and [2021-02-10](https://tzstats.com/docs/api#2021-02-10) (the new v9.1 branch of TzIndex PRO contains all changes from both API upgrades).


## 9.0.1

- ETL: fix undocumented vesting balance update

## 9.0.0

**Tezos Florence Protocol Support**

- Florence v009 `PsFLorena` protocol and Florencenet
- renamed voting periods to `proposal`, `exploration`, `cooldown`, `promotion` and `adoption`
- renamed voting-related API data types accordingly (`/explorer/election`)
- new protocol operation `failing_noop` usable for message signatures without effect on on-chain state
- new `endorsement_with_slot` data model
- handle balance receipts from migrations

**Fixes and Improvements**

- ETL: fix vote state rollback on cycle end
- BASE58: fix overflow panic on ill-formed base58 strings
- CLI: new `--notls` argument to disable RPC TLS when no protocol is specified in the RPC URL (also available as env `TZ_RPC_DISABLE_TLS`)
- CLI: new `--insecure` argument to disable RPC TLS certificate checks (also available as env `TZ_RPC_INSECURE_TLS`)

## 8.0.3

- ETL: fixed delegation flows from internal origination operations

## 8.0.2

- API: add missing adoption period info
- Micheline: Fix Zarith encoder emitting wrong binary for numbers between 32 and 63
- ETL: improve bigmap indexing by skipping hash index lookups
- API: improve bigmap query performance

## 8.0.1

Bugfix release

- ETL: fix protocol detection for Edo

## 8.0.0

Note: compatible with Edo and API version 2020-06-01. Database rebuild is required.

- ETL: Tezos Edo protocol and Edonet support including Edo2 (PtEdo2Zk) and Edonet2
- ETL: support internal origination+delegation
- ETL: new `light` mode that skips baker and governance data (cli arg `--light`)
- ETL: new `validate` mode for checking balances and account state each block
- ETL: improved decimal conversion performance
- ETL: improved base58 encode/decode performance
- ETL: improved database performance
- ETL: added multiple caches to improve indexer and query performance
- ETL: delegation operations now contain the delegated balance as volume
- ETL: fixed `is_delegatable` and `is_spendable` flags for accounts and contracts
- ETL: fixed removing accounts on rollback
- ETL: fixed roll calculations for index 15 snapshots
- ETL: fixed baker registrations due to Tezos protocol bug in v002
- ETL: fixed call stats for migrated contracts
- API: new call dispatcher to limit concurrent requests and return 429 on overflow
- API: new governance endpoints to list voters `/explorer/election/:num/:stage/voters` and ballots `/explorer/election/:num/:stage/ballots`
- API: new account rank endpoints `/explorer/rank/volume`, `/explorer/rank/traffic` and `/explorer/rank/balances`
- API: improved cache expiry
- API: improved operation list performance
- API: improved Edo comb pair unwrapping

**Tezos Edo Protocol Support**

- introduced a 5th voting period (adoption) and decreased duration of each period
- RPC changes for block metadata (level and voting info)
- RPC changes for bigmap updates (now called lazy storage)
- new Michelson `comb pairs` and related data encoding
- new Michelson `tickets` and related data encoding
- new Michelson instructions for BLS curves, Sapling and tickets

## 7.0.0

Note: compatible with Delphi and API version 2020-06-01. Database rebuild is required.

**FIXES**

- ETL: fixed params init for end of cycle check in builder
- ETL: fixed protocol version lookup and deployment numbers
- ETL: fixed snapshot deadlock
- ETL: fixed temporary bigmap allocation
- ETL: fixed short bigmap type rendering
- ETL: fixed params init for end of cycle check in builder
- ETL: fixed token generation counter for bake and aidrop
- ETL: fixed calculating performance for small bakers
- ETL: fixed protocol version lookup and deployment numbers
- ETL: fixed multiple race conditions
- ETL: fixed traffic rank sorting
- ETL: fixed return data from bigmap copy operations
- ETL: don't update source when delegating twice to the same baker
- ETL: fixed reporting volume from failed transactions in block summary
- ETL: handle out-of-bounds RFC3339 timestamps (return integer when year > 9999)
- API: fixed returning operations with bigmap remove action
- API: don't return origination when entrypoint filter is used in contract call list
- API: fixed cache expiration time for account, contract, rights
- API: fixed bigmap pair key stringifier
- API: fixed cache expiration time for account, contract, rights
- API: fixed bigmap action listing
- API: fixed contract lookup by multiple interface hashes
- API: fixed account table timestamp loading when columns are limited
- API: fixed last endorsed block lookup

**FEATURES**

- ETL: add nonce hash type
- ETL: add Delphi protocol and Delphinet support
- ETL: add implicit baker registrations to operation table
- API: optimize access to chain tip and parameters
- API: list related delegations when account is delegate
- API: support temporary bigmaps

## 6.0.3

**FIXES**

- ETL: fixed bigmap copy duplicating temporary bigmap entries
- ETL: fixed reporting volume from failed transactions in block summary
- Micheline: Handle entrypoint-suffixed addresses
- API: Hotfix for broken entrypoint detection

**FEATURES**

- API: added snapshot time to cycle

## 6.0.2

**FIXES**

- ETL: fix delegation rollback for new delegators
- API: simplify ballot list return values, add missing sender
- API: fix `since` off-by-one bug on contract call lists
- contract calls: include non-param transactions
- chain: fix binary key decoding
- micheline: be resilient to ill-formed programs
- etl: fix delegation lookup during reorg
- bigmap: properly handle bool keys

**NEW FEATURES**

- added `--nomonitor` CLI switch to disable calls to Tezos node monitor enpoints

## 6.0.1

- micheline: hotfix for ingesting pair keys on Carthagenet

## v6.0.0

## POTOCOL UPGRADE
- supports Carthage protocol PsCARTHAGazKbHtnKfLzQg3kms52kSRpgnDY982a9oYsSXRLQEb
- supports new Carthage reward constants for Emmy+ called `baking_reward_per_endorsement` and `endorsement_reward` in the Tezos RPC
- updated expected income based on new rewards formula

## NEW EXPLORER FEATURES
- `/explorer/config/{height}` now returns two additional float arrays `block_rewards_v6` and `endorsement_rewards_v6` containing the new Carthage reward constants; the previous fields `block_reward` and `endorsement_reward` remain unchanged and will contain the first elements from the corresponding v6 arrays

## v5.3.3

- changed copyright owner to Blockwatch Data Inc.

## v5.3.2

- add --enable-cors switch for local mode
- change proof_of_work_threshold type to int64
- fix rpc url parsing
- fix for empty time config in zeronet

## v5.3.1

- update packdb to flush table metadata on journal flush

## v5.3.0

### CLI CHANGES
- support url paths in `--rpcurl`

### FIXES
- fixed empty cycle response on zero supply
- voting period start and end heights are no longer off by 1
- voting quorum, ema and eligible rolls calculations are corrected
- improved smart contract entrypoint detection so that annotated parent nodes in the parameter primitive tree are no longer shadowing valid entrypoints

### NEW EXPLORER FEATURES
- listing account ops supports `order`, `block` and `since` query arguments
- extended op fields `paramaters`, `storage` and `big_map_diff` to include unboxed types and values and made prim tree optional
- added new contract endpoints
	- `/explorer/contract/{addr}/calls` to list smart contract calls
	- `/explorer/contract/{addr}/manager` current manager account (originator in v005)
	- `/explorer/contract/{addr}/script` code, storage &  parameter types
	- `/explorer/contract/{addr}/storage` current storage state
- added contract field `bigmap_ids` to list ids of bigmaps owned by this contract
- added bigmap endpoints
	- `/explorer/bigmap/{id}` bigmap metadata
	- `/explorer/bigmap/{id}/type` bigmap type definition
	- `/explorer/bigmap/{id}/keys` list bigmap keys
	- `/explorer/bigmap/{id}/values` list bigmap key/value pairs
	- `/explorer/bigmap/{id}/updates` list bigmap updates
	- `/explorer/bigmap/{id}/{key}` single bigmap value
	- `/explorer/bigmap/{id}/{key}/updates` list updates for a single bigmap value
- add network health estimation based on recent 128 blocks (priority, endorsements, reorgs)

### NEW TABLE FEATURES
- added bigmap table `/tables/bigmap` to access raw bigmap updates

### DEPRECATION NOTICES
- removed deprecated [contract](#contracts) field `ops` and endpoint `/explorer/contract/{addr}/op` (use `/explorer/account/{addr}/op` endpoint instead)
- removed deprecated [contract](#contracts) fields `delegate`, `manager`, `script` (use new endpoints or related account endpoints instead)


## v5.2.0

### CLI CHANGES
- run arguments `host`, `user` and `pass` have been renamed to `rpcurl`, `rpcuser` and `rpcpass`
- default RPC URL has changed from https://127.0.0.1:8732 to http://127.0.0.1:8732 to prefer non-TLS mode for local connections

### FIXES
- account `is_revealed` is now correctly reset when account balance becomes zero (in this case a Tezos node will remove all account data including a revealed pubkey from storage)
- eligible voting rolls are now taken after cycle start block is processed
- counting duplicate proposal votes has been corrected
- annualized supply calculation has been fixed to use 365 instead of 364 days
- fixed vote table  `period_start_height` and `period_end_height` field names
- fixed empty fields in some CSV results
- numeric filters on tables now fully support range, in and not-in argument lists
- corrected `income.missed_baking_income` when prio 0 blocks are lost
- corrected `supply.circulating` to contain all coins that can move next block (= total - unvested)
- support big integers in big_map_diffs
- correctly handle origination big_map_diffs
- count income contribution_pct to measure rights utilized (contrib to consensus)
- renamed income efficiency/efficiency_pct into performance to measure roi on staking capital
- support negative baker efficieny when slashed (efficiency = 0)
- count double bake/endorse events instead of accusation ops for cycle metrics
- block operation list paging with offset/limit now properly counts internal and batch operations

### NEW EXPLORER FEATURES
- added config field `deployment` that contains a serial counter of protocol activations on the chain
- changed config field `version` to show the protocol implementation version (ie. 4 for Athens, 5 for Babylon, etc)
- added block field `successor`
- added op fields `branch_id`, `branch_height`, `branch_depth`, `branch`

### NEW TABLE FEATURES
- added op table fields `branch_id`, `branch_height`, `branch_depth`
- changed CSV timestamps to RFC3339 (from UNIX milliseconds)

### DEPRECATION NOTICES
- removed income table fields `efficiency_percent`, `slashed_income` and `lost_baking_income`
- removed unused reports feature

## v5.1.0

*unreleased, skipped*

- update spendable/delegatable flag handling
- correct list of babylon airdrop receivers
- account for missed seed nonces
- update on-chain constants in each cycle
- update baker income and luck during rampup cycles
- fix cache max-age header
- make explorer tip and config cacheable
- store contract originator as manager (Babylon)
- store merge-activated account in op table (this re-used the manager field before)
