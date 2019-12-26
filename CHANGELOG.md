# Changelog

### FIXES

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


### NEW TABLE FEATURES
- added bigmap table `/tables/bigmap` to access raw bigmap updates

### DEPRECATION NOTICES


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