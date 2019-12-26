// Copyright (c) 2019 KIDTSUNAMI
// Author: alex@kidtsunami.com

package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/spf13/cobra"

	"blockwatch.cc/packdb/pack"
	"blockwatch.cc/packdb/store"
	"blockwatch.cc/tzindex/etl"
	"blockwatch.cc/tzindex/etl/index"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/rpc"
	"blockwatch.cc/tzindex/server"
	"github.com/echa/config"
)

var (
	noindex bool
	unsafe  bool
	norpc   bool
	noapi   bool
	stop    int64
)

func init() {
	runCmd.Flags().StringVar(&rpcurl, "rpcurl", "http://127.0.0.1:8732", "RPC url")
	runCmd.Flags().StringVar(&rpcuser, "rpcuser", "", "RPC username")
	runCmd.Flags().StringVar(&rpcpass, "rpcpass", "", "RPC password")
	runCmd.Flags().BoolVar(&norpc, "norpc", false, "disable RPC client")
	runCmd.Flags().BoolVar(&noapi, "noapi", false, "disable API server")
	runCmd.Flags().BoolVar(&noindex, "noindex", false, "disable indexing")
	runCmd.Flags().BoolVar(&unsafe, "unsafe", false, "disable fsync for fast ingest (DANGEROUS! data will be lost on crashes)")
	runCmd.Flags().Int64Var(&stop, "stop", 0, "stop indexing after `height`")

	rootCmd.AddCommand(runCmd)
}

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run as service",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runServer(args); err != nil {
			log.Fatalf("Fatal: %v", err)
		}
	},
}

func runServer(args []string) error {
	// overwrite config from flags
	if err := parseRPCFlags(); err != nil {
		return err
	}

	// set user agent in library client
	server.UserAgent = UserAgent
	server.ApiVersion = API_VERSION
	pack.QueryLogMinDuration = config.GetDuration("database.log_slow_queries")

	engine := config.GetString("database.engine")
	pathname := config.GetString("database.path")
	log.Infof("Using %s database %s", engine, pathname)
	if unsafe {
		log.Warnf("Enabled NOSYNC mode. Database will not be safe on crashes!")
	}

	// open shared state database
	statedb, err := store.Create(engine, filepath.Join(pathname, etl.StateDBName), DBOpts(engine, false, unsafe))
	if err != nil {
		return fmt.Errorf("error opening %s database: %v", etl.StateDBName, err)
	}
	defer statedb.Close()

	// open RPC client when requested
	var rpcclient *rpc.Client
	if !norpc {
		rpcclient, err = newRPCClient()
		if err != nil {
			return err
		}
	} else {
		noindex = true
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// enable index storage tables
	indexer := etl.NewIndexer(etl.IndexerConfig{
		DBPath:  pathname,
		DBOpts:  DBOpts(engine, false, unsafe),
		StateDB: statedb,
		Indexes: []model.BlockIndexer{
			index.NewAccountIndex(tableOptions("account"), indexOptions("account")),
			index.NewContractIndex(tableOptions("contract"), indexOptions("contract")),
			index.NewBlockIndex(tableOptions("block"), indexOptions("block")),
			index.NewOpIndex(tableOptions("op"), indexOptions("op")),
			index.NewFlowIndex(tableOptions("flow")),
			index.NewChainIndex(tableOptions("chain")),
			index.NewSupplyIndex(tableOptions("supply")),
			index.NewRightsIndex(tableOptions("right")),
			index.NewSnapshotIndex(tableOptions("snapshot")),
			index.NewIncomeIndex(tableOptions("income")),
			index.NewGovIndex(tableOptions("gov")),
			index.NewBigMapIndex(tableOptions("bigmap"), indexOptions("bigmap")),
		},
	})
	defer indexer.Close()

	crawler := etl.NewCrawler(etl.CrawlerConfig{
		DB:        statedb,
		Indexer:   indexer,
		Client:    rpcclient,
		Queue:     config.GetInt("crawler.queue"),
		StopBlock: stop,
		Snapshot: &etl.SnapshotConfig{
			Path:          config.GetString("crawler.snapshot_path"),
			Blocks:        config.GetInt64Slice("crawler.snapshot_blocks"),
			BlockInterval: config.GetInt64("crawler.snapshot_interval"),
		},
	})
	// not indexing means we do not auto-index, but allow access to
	// existing indexes
	if !noindex {
		if err := crawler.Init(ctx, etl.MODE_SYNC); err != nil {
			return fmt.Errorf("error initializing crawler: %v", err)
		}
		crawler.Start()
		defer crawler.Stop(ctx)
	} else {
		if err := crawler.Init(ctx, etl.MODE_INFO); err != nil {
			return fmt.Errorf("error initializing crawler: %v", err)
		}
	}

	// setup HTTP server
	if !noapi {
		srv, err := server.New(&server.Config{
			Crawler: crawler,
			Indexer: indexer,
			Client:  rpcclient,
			Http: server.HttpConfig{
				Addr:                config.GetString("server.addr"),
				Port:                config.GetInt("server.port"),
				Scheme:              config.GetString("server.scheme"),
				Host:                config.GetString("server.host"),
				MaxWorkers:          config.GetInt("server.workers"),
				TimeoutHeader:       config.GetString("server.timeout_header"),
				ReadTimeout:         config.GetDuration("server.read_timeout"),
				HeaderTimeout:       config.GetDuration("server.header_timeout"),
				WriteTimeout:        config.GetDuration("server.write_timeout"),
				KeepAlive:           config.GetDuration("server.keepalive"),
				ShutdownTimeout:     config.GetDuration("server.shutdown_timeout"),
				DefaultListCount:    config.GetUint("server.default_list_count"),
				MaxListCount:        config.GetUint("server.max_list_count"),
				DefaultExploreCount: config.GetUint("server.default_explore_count"),
				MaxExploreCount:     config.GetUint("server.max_explore_count"),
				CorsEnable:          config.GetBool("server.cors_enable"),
				CorsOrigin:          config.GetString("server.cors_origin"),
				CorsAllowHeaders:    config.GetString("server.cors_allow_headers"),
				CorsExposeHeaders:   config.GetString("server.cors_expose_headers"),
				CorsMethods:         config.GetString("server.cors_methods"),
				CorsMaxAge:          config.GetString("server.cors_maxage"),
				CorsCredentials:     config.GetString("server.cors_credentials"),
				CacheEnable:         config.GetBool("server.cache_enable"),
				CacheControl:        config.GetString("server.cache_control"),
			},
		})
		if err != nil {
			return err
		}
		srv.Start()
		defer srv.Stop()
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	<-c
	return nil
}
