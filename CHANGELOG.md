# Changelog

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
