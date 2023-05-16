# Blockwatch Tezos Indexer

© 2020-2023 Blockwatch Data Inc., All rights reserved.

All-in-one zero-conf blockchain indexer for Tezos. A fast, convenient and resource friendly way to gain tactical insights and build dapps on top of Tezos. Supported by [Blockwatch Data](https://blockwatch.cc), Pro version available on request.

For support, talk to us on [Twitter](https://twitter.com/tzstats) or [Discord](https://discord.gg/D5e98Hw).

**Core Features**

- supports protocols up to Mumbai (v016)
- indexes and cross-checks full on-chain state
- feature-rich [REST API](https://tzstats.com/docs/api/index.html) with objects, bulk tables and time-series
- auto-detects and locks Tezos network (never mixes data from different networks)
- indexes all accounts and smart-contracts (including genesis data)
- follows chain reorgs as they are resolved
- can passively monitor for new blocks
- self-heals broken node connections (retries until node RPC comes back up)
- API supports CORS and HTTP caching
- high-performance embedded data-store
- flexible in-memory caching for fast queries
- automatic database backups/snapshots
- configurable HTTP request rate-limiter
- flexible metadata support

**Supported indexes and data tables**

- **blocks**: all blocks including orphans, indexed by hash and height
- **operations**: all on-chain operations including contract call data, indexed by hash ( endorsements are split into separate table, only available in full mode)
- **accounts**: running account details like most recent balances, indexed by address
- **balances**: end-of-block balance history for all addresses
- **contracts**: running smart contract details, code and initial storage
- **flows**: complete list of balance, freezer and delegation balance updates
- **chain**: running blockchain totals
- **supply**: running supply totals
- **bigmap**: bigmap smart contract storage index
- **rights**: compact representations of assigned and used baking and endorsing rights
- **elections**, **votes**, **proposals** and **ballots** capturing all on-chain governance activities
- **snapshots**: balances of active delegates & delegators at all snapshot blocks
- baker **income**: per-cycle statistics on baker income, efficiency, etc
- **metadata**: standardized and custom account/token metadata
- **constants**: global constants (e.g. smart contract code/type macros to lower contract size and reuse common features)
- **storage**: separate smart contract storage updates to decrease operation table cache pressure
- **event**: emitted smart contract events
- **tickets**: emitted smart contract ticket updates

**Operation modes**

- **Light** (default) light-weight mode without consensus and governance data (CLI: `--light`)
- **Full** regular operation mode that builds all indexes (CLI: `--full`)
- **Validate** state validation mode for checking accounts and balances each block/cycle (CLI: `--validate`)

**Light mode** dramatically reduces our maintenance costs for TzIndex and is best suited for dapps where access to baking-related data is not necessary. Light mode saves roughly \~50% storage costs and \~50% indexing time while still keeping all data required for Dapps.

**Validate mode** works in combination with full and light mode. At each block it checks balances and states of all touched accounts against a Tezos archive node before any change is written to the database. At the end of each cycle, all known accounts in the indexer database are checked as well. This ensures 100% consistency although at the cost of a reduction in indexing speed.


### Requirements

- Storage: 43GB (full Mainnet index, May 2023), 29G (light mode)
- RAM:  8-64GB (configurable, use more memory for better query latency)
- CPU:  2+ cores (configurable, use more for better query parallelism)
- Tezos node in archive mode

Runs against any Tezos Archive Node (also full nodes when cycle 0 history is not yet pruned). This can be a local node or one of the public Tezos RPC nodes on the Internet. Note that syncing from public nodes over the Internet works but may be slow.

**IMPORTANT: WHEN USING OCTEZ V12+ YOU MUST RUN YOUR ARCHIVE NODE WITH `--metadata-size-limit unlimited`**

Requires access to the following Tezos RPC calls

```
/chains/main/blocks/{blockid}
/chains/main/blocks/{blockid}/helpers/baking_rights (full mode only)
/chains/main/blocks/{blockid}/helpers/endorsing_rights (full mode only)
/chains/main/blocks/{blockid}/context/selected_snapshot (Ithaca+ full mode only)
/chains/main/blocks/{blockid}/context/raw/json/cycle/{cycle} (full mode only)
/chains/main/blocks/{blockid}/context/constants
/chains/main/blocks/head/header
/monitor/heads/main (optional)
/chains/main/blocks/{blockid}/context/contract/{address} (validate mode only)
/chains/main/blocks/{blockid}/context/delegates/{address} (validate mode only)
```

### How to build

The contained Makefile supports local and Docker builds. For local builds you need a recent Golang distribution installed on your system.

```
# build a binary for your OS
make build

# or directly with Go
go build ./cmd/tzindex.go

# build docker images
make image
```

### How to run

tzindex aims to be zero-conf and comes with sane defaults. All you need to do is point it to the RPC endpoint of a running Tezos node.

```
tzindex -rpc.url tezos-node
```

If you prefer running from docker, check out the docker directory. Official images are available for the [indexer](https://hub.docker.com/r/blockwatch/tzindex) and [frontend](https://hub.docker.com/r/blockwatch/tzstats) (note the frontend may not support advanced features of new protocols). You can run both, the indexer and the frontend in local Docker containers and have them connect to your Tezos node in a third container. Make sure all containers are connected to the same Docker network or if you choose different networks that they are known. Docker port forwarding on Linux usually works, on OSX its broken.


### Configuration

**Config file**

On start-up tzindex tries loading its config from the `config.json` file in the current directory. You may override this name either on the command line using `-c myconf.json` or by setting the environment variable `TZ_CONFIG_FILE=/some/path/myconf.json`

Config file sections
```
rpc      - configures RPC connection to the Tezos node
crawler  - configures the blockchain crawl logic
db       - configures the embedded database
server   - configures the built-in HTTP API server
log      - configures logging for all subsystems
```

See the default `config.json` in the `docker` subfolder for a detailed list of all settings.

**Environment variables**

Env variables allow you to override settings from the config file or even specify all configuration settings in the process environment. This makes it easy to manage configuration in Docker and friends. Env variables are all uppercase, start with `TZ` and use an underscore `_` as separator between sub-topics.

```
# in config.json
{ "rpc": { "url" : "http://127.0.0.1:8732" }}

# same as env variable
TZ_RPC_URL=http://127.0.0.1:8732
```

**Command line arguments**

All TzIndex config options can be controlled via cli arguments.

Global options:

```
Usage: tzindex [flags]

Flags
  -c file
      read config from file (default "config.json")
  -config file
      read config from file (default "config.json")
  -enable-cors
      enable API CORS support
  -full
      full mode (including baker and gov data)
  -insecure
      disable RPC TLS certificate checks (not recommended)
  -light
      light mode (use to skip baker and gov data) (default true)
  -noapi
      disable API server
  -noindex
      disable indexing
  -nomonitor
      disable block monitor
  -norpc
      disable RPC client
  -notls
      disable RPC TLS support (use http)
  -stop height
      stop indexing after height
  -unsafe
      disable fsync for fast ingest (DANGEROUS! data will be lost on crashes)
  -v  be verbose
  -validate
      validate account balances
  -version
      show version
  -vv
      debug mode
  -vvv
      trace mode
```

Subsystem Options

```
Database
  -db.path=./db             path for database storage
  -db.log_slow_queries=1s   warn when DB queries take longer than this

Go runtime
  -go.cpu=0            max number of CPU cores to use (0 = all)
  -go.gc=20            trigger GC when used mem grows by N percent
  -go.sample_rate=0    block and mutex profiling sample rate (0 = off)

Crawler
  -crawler.cache_size_log2=15                max number of cached accounts when crawling
  -crawler.queue=100                         max number of blocks to prefetch
  -crawler.delay=1                           offset from chain head (use 1 or 2 for reorg safe indexing)
  -crawler.snapshot.path=./db/snapshot       target path for indexer database snapshots
  -crawler.snapshot.blocks=height1,height2   target blocks to create snapshots
  -crawler.snapshot.interval=0               interval between blocks to create snapshots

Server
  -server.addr=127.0.0.1            server listen address
  -server.port=8000                 server listen port
  -server.workers=64                number of Goroutines for executing API queries
  -server.queue=128                 number of open requests to queue for execution
  -server.read_timeout=5s           max timeout for receiving complete requests
  -server.header_timeout=2s         max timeout for receiving request headers
  -server.write_timeout=90s         max timeout for sending replies
  -server.keepalive=90s             connection keep alive time
  -server.shutdown_timeout=60s      delay to wait for draining open requests on shutdown
  -server.max_list_count=500000     max number of result rows for table queries
  -server.default_list_count=500    default number of result rows for table queries
  -server.max_series_duration=0     max time-series duration per request
  -server.max_explore_count=100     max number or explorer API results in lists
  -server.default_explore_count=20  default number of results in explorer API lists
  -server.cors_enable=false         add CORS response headers
  -server.cors_origin=*             CORS origin header contents
  -server.cors_allow_headers=       CORS allow header contents
  -server.cors_expose_headers=      CORS expose header contents
  -server.cors_methods=             CORS methods header contents
  -server.cors_maxage=              CORS maxage header contents
  -server.cors_credentials=         CORS credentials header contents
  -server.cache_control=public      cache control header contents
  -server.cache_expires=30s         default cache expiry time for mutable API responses
  -server.cache_max=24h             max cache expiry time for immutable API responses

RPC
  -rpc.url=http://127.0.0.1:8732    Tezos RPC host
  -rpc.disable_tls=true             use HTTP by default
  -rpc.insecure_tls=false           disable TLS certificate checks
  -rpc.proxy=                       set HTTP proxy
  -rpc.proxy_user=                  optional HTTP proxy username
  -rpc.proxy_pass=                  optional HTTP proxy password
  -rpc.dial_timeout=10s             max connection delay before failing
  -rpc.keepalive=30m                connection keep alive
  -rpc.idle_timeout=30m             close connections when idle for longer
  -rpc.response_timeout=60m         max delay waiting for RPC responses
  -rpc.continue_timeout=60s         max delay waiting for HTTP chunks
  -rpc.idle_conns=16                max server connections

Logging
  -log.progress=10s                 interval for progress logs
  -log.backend=stdout               log backend (stdout, stderr, syslog, file)
  -log.flags= date,time,micro,utc   log flags (see Golang log package)
  -log.level=info                   default global log level
  -log.etl=info                     log level for chain crawler
  -log.db=info                      log level for database layer
  -log.rpc=info                     log level for RPC layer
  -log.api=info                     log level for API server
  -log.micheline=info               log level for TzGo micheline package
```

### License

This Software is available under two different licenses, the open-source **MIT** license with limited support / best-effort updates and a **PRO** license with professional support and scheduled updates. The professional license is meant for businesses such as dapps, marketplaces, staking services, wallet providers, exchanges, asset issuers, and auditors who would like to use this software for their internal operations or bundle it with their commercial services.

The PRO licenses helps us pay for software updates and maintenance and operate the free community services on tzstats.com.

The following table may help you pick the right license for your intended use-case. If in doubt, send an email to license@blockwatch.cc and we'll get back to you.


| | MIT | PRO |
|-|---------------|----------------|
| Costs | Free | Subscription |
| Type | Perpetual | Perpetual |
| Use | Any | Commercial Use |
| Mode | All | All |
| Limitations | - | See Agreement |
| Support | Best effort | Commercial Support Available |
| Upgrades | Best-effort | Early |
| Health check | No | Available |
| API statistics | No | Available |
| ZMQ | No | Available |
| RPC Proxy | No | Available |
| QA Tools | No | Available |

