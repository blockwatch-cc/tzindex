# Blockwatch Tezos Indexer

© 2019 KIDTSUNAMI UG, All rights reserved.

All-in-one zero-conf blockchain indexer for Tezos. A fast, convenient and resource friendly way to gain tactical insights and build dapps on top of Tezos.

**Core Features**

- indexes and cross-checks full on-chain state
- feature-rich [REST API](https://tzstats.com/docs/api/index.html) with objects, bulk tables and time-series
- supports protocols up to Babylon 2.0.1 (v005)
- auto-detects and locks Tezos network (never mixes data from different networks)
- indexes all accounts and smart-contracts (including genesis data)
- follows chain reorgs as they are resolved
- can passively monitor for new blocks
- self-heals broken node connections (retries until node RPC comes back up)
- API supports CORS and HTTP caching
- high-performance embedded data-store
- flexible in-memory caching for fast queries
- automatic database backups/snapshots


**Supported indexes and data tables**

- **blocks**: all blocks including orphans, indexed by hash and height
- **operations**: all on-chain operations including contract call data, indexed by hash
- **accounts**: running account details like most recent balances, indexed by address
- **contracts**: running smart contract details, code and initial storage
- **flows**: complete list of balance, freezer and delegation balance updates
- **chain**: running blockchain totals
- **supply**: running supply totals
- **rights**: full list of all baking and endorsing rights
- **elections**, **votes**, **proposals** and **ballots** capturing all on-chain governance activities
- **snapshots**: balances of active delegates & delegators at all snapshot blocks
- baker **income**: per-cycle statistics on baker income, efficiency, etc


### Requirements

- Storage: 2GB (full Mainnet index)
- RAM:  4-16GB (configurable, use more memory for better query latency)
- CPU:  2+ cores (configurable, use more for better query parallelism)
- Tezos archive node

Runs against any Tezos Archive Node (also full nodes when cycle 0 history is not yet pruned). This can be a local node or one of the public Tezos RPC nodes on the Internet. Note that syncing from public nodes over the Internet works but may be slow.

Requires access to the following Tezos RPC calls

```
/chains/main/blocks/{blockid}
/chains/main/blocks/{blockid}/helpers/baking_rights
/chains/main/blocks/{blockid}/helpers/endorsing_rights
/chains/main/blocks/{blockid}/context/raw/json/cycle/{cycle}
/chains/main/blocks/{blockid}/context/constants
/chains/main/blocks/head/header
/monitor/heads/main (optional)
```

### How to run

tzindex aims to be zero-conf and comes with sane defaults. All you need to do is point it to the RPC endpoint of a running Tezos node.

```
tindex run --host tezos-node
```

If you prefer running from docker images, check out the docker directory. Official images are available for the [indexer](https://hub.docker.com/r/blockwatch/tzindex) and [frontend](https://hub.docker.com/r/blockwatch/tzstats). You can run both, the indexer and the frontend in local Docker containers and have them connect to your Tezos node in a third container. Make sure all containers run inside the same Docker network or if you choose different networks that they are known. Port forwarding to localhost may or may not be an issue.


### Configuration

**Config file**

On start-up tzindex tries loading its config from the `config.json` file in the current directory. You may override this name either on the command line using `-c myconf.json` or by setting the environment variable `TZ_CONFIG_FILE=/some/path/myconf.json`

Config file sections
```
rpc      - configures RPC connection to the Tezos node
crawler  - configures the blockchain crawl logic
database - configures the embedded database
server   - configures the built-in HTTP API server
logging  - configures logging for all subsystems
```

See the default `config.json` in the `docker` subfolder for a detailed list of all settings.

**Environment variables**

Env variables allow you to override settings from the config file or even specify all configuration settings in the process environment. This makes it easy to manage configuration in Docker and friends. Env variables are all uppercase, start with `TZ` and use an underscore `_` as separator between sub-topics.

```
# in config.json
{ "rpc": { "host" : "127.0.0.1" }}

# same as env variable
TZ_RPC_HOST=127.0.0.1
```

**Command line arguments**

A few global settings can only be controlled via cli arguments:

```
Usage:
  tzindex [command]

Available Commands:
  help        Help about any command
  run         Run as service
  version     Print the version number of tzindex

Flags:
  -c, --config string          config file
      --cpus int               max number of logical CPU cores to use (default: all) (default -1)
  -p, --dbpath path            database path
      --gogc int               trigger GC when used mem grows by N percent (default 20)
  -h, --help                   help for tzindex
      --profile-block string   write blocking events to file
      --profile-cpu string     write cpu profile to file
      --profile-mutex string   write mutex contention samples to file
      --profile-rate int       block/mutex profiling rate in fractions of 100 (e.g. 100 == 1%) (default 100)
  -t, --test                   test configuration and exit
      --v                      be verbose
      --vv                     debug mode
      --vvv                    trace mode
```

The main `tzindex run` command has a few additional options

```
Usage:
  tzindex run [flags]

Flags:
  -h, --help          help for run
      --host string   RPC hostname (default "127.0.0.1")
      --noapi         disable API server
      --noindex       disable indexing
      --norpc         disable RPC client
      --pass string   RPC password
      --port string   RPC port
      --stop height   stop indexing after height
      --unsafe        disable fsync for fast ingest (DANGEROUS! data will be lost on crashes)
      --user string   RPC username
```

### License

This Software is available under two different licenses, a **Community Edition** license and a **PROfessional** license. The community license is intended for personal use and for developers and businesses who like to try out new ideas but don't make any money yet. The professional license is meant for businesses that already earn money with their services (such as staking providers, wallet providers, exchanges, funds and auditors) and would like to use this software for their internal operations or even make it part of their commercial offering.

Commercial licenses help us update and maintain the software itself and the community services like Tzstats.com which build on it.

The following table may help you pick the right license for your intended use-case. If in doubt, send an email to license@kidtsunami.com and we'll get back to you.


| | KIDTSUNAMI CE | KIDTSUNAMI PRO |
|-|---------------|----------------|
| Price | Free | 199 EUR / protocol upgrade |
| Type | Perpetual | Perpetual |
| Use | Personal & Academic Use | Commercial Use |
| Limitations | No resale, no services, no ads | - |
| Support | Best effort | Commercial Support |
| Protocol Upgrade Availability | 1 week before activation | 1 week after proposal period ends |
| DB Rebuild on Upgrade | Required | Integrated |
| RPC Proxy | No | Available |
| QA Tools | No | Available |

