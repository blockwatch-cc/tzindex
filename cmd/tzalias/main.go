// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/qri-io/jsonschema"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzpro-go/tzpro"
	"blockwatch.cc/tzpro-go/tzpro/index"
	"github.com/echa/config"
	"github.com/echa/log"
)

var (
	flags    = flag.NewFlagSet("tzalias", flag.ContinueOnError)
	verbose  bool
	sorted   bool
	nobackup bool
	nocolor  bool
	apiurl   string
)

const defaultFilePrefix = "tzalias-export"

func init() {
	flags.Usage = func() {}
	flags.BoolVar(&verbose, "v", false, "be verbose")
	flags.BoolVar(&sorted, "sorted", false, "sort JSON file by alias name")
	flags.BoolVar(&nobackup, "no-backup", false, "don't backup data before destructive commands")
	flags.BoolVar(&nocolor, "no-color", false, "disable color output")
	flags.StringVar(&apiurl, "index", "http://localhost:8000", "Index API URL")
}

func main() {
	if err := flags.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			fmt.Println("Usage: tzalias <cmd> [address[/asset_id]] [fields] [<file>]")
			flags.PrintDefaults()
			fmt.Println("\nCommands")
			fmt.Printf("  export          download and save metadata to `file`\n")
			fmt.Printf("  import          import all metadata from `file`\n")
			fmt.Printf("  inspect         show metadata\n")
			fmt.Printf("  add             add new metadata\n")
			fmt.Printf("  update          update existing metadata\n")
			fmt.Printf("  remove          removes existing metadata (DESTRUCTIVE!)\n")
			fmt.Printf("  purge           drop all metadata (DESTRUCTIVE!)\n")
			fmt.Println("\nFields")
			fmt.Printf("  alias.name            (string) account/token display name\n")
			fmt.Printf("  alias.kind            (enum) e.g. validator, payout, token, oracle, issuer, registry, ...\n")
			fmt.Printf("  alias.description     (string) short account description\n")
			fmt.Printf("  alias.category        (string) user-defined category\n")
			fmt.Printf("  alias.logo            (string) logo image URL\n")
			fmt.Printf("  baker.status          (enum) validator status (public, private, closing, closed)\n")
			fmt.Printf("  baker.fee             (float) baker fee [0.0 .. 1.0]\n")
			fmt.Printf("  baker.sponsored       (bool) paid advertisement\n")
			fmt.Printf("  baker.payout_delay    (bool) baker delays payout until unfreeze\n")
			fmt.Printf("  baker.min_payout      (float) minimum payout amount\n")
			fmt.Printf("  baker.min_delegation  (float) minimum delegation requirement\n")
			fmt.Printf("  baker.non_delegatable (bool) baker does not support public delegation\n")
			fmt.Printf("  payout                ([]string) list of related baker addresses, only for kind payout\n")
			fmt.Printf("  location.country      (enum) ISO 2-letter country code\n")
			fmt.Printf("  location.city         (enum) IATA 3-letter airport code\n")
			fmt.Printf("  location.lat          (float) GPS latitude\n")
			fmt.Printf("  location.lon          (float) GPS longitude\n")
			fmt.Printf("  location.alt          (float) GPS altitude\n")
			fmt.Printf("  social.twitter        (string) twitter handle\n")
			fmt.Printf("  social.reddit         (string) reddit handle\n")
			fmt.Printf("  social.github         (string) github handle\n")
			fmt.Printf("  social.instagram      (string) instagram handle\n")
			fmt.Printf("  asset.standard        (string) token standard\n")
			fmt.Printf("  asset.symbol          (string) token symbol\n")
			fmt.Printf("  asset.decimals        (int) token decimals\n")
			fmt.Printf("  asset.version         (string) smart contract version\n")
			fmt.Printf("  asset.homepage        (string) token issuer homepage\n")
			fmt.Printf("  asset.tags            ([]string) list of user-defined tags\n")
			fmt.Printf("  updated.hash          (string) last update block\n")
			fmt.Printf("  updated.height        (int) last update height\n")
			fmt.Printf("  updated.time          (string) last update time\n")
			os.Exit(0)
		}
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	config.SetEnvPrefix("TZALIAS")
	realconf := config.ConfigName()
	if _, err := os.Stat(realconf); err == nil {
		if err := config.ReadConfigFile(); err != nil {
			fmt.Printf("Could not read config %s: %v\n", realconf, err)
			os.Exit(1)
		}
		log.Infof("Using configuration file %s", realconf)
	} else {
		log.Warnf("Missing config file, using default values.")
	}

	if verbose {
		log.SetLevel(log.LevelDebug)
	}

	if err := run(); err != nil {
		if e, ok := tzpro.IsErrApi(err); ok {
			log.Errorf("%s: %s", e.Errors[0].Message, e.Errors[0].Detail)
		} else {
			log.Error(err)
		}
		os.Exit(1)
	}
}

func run() error {
	if flags.NArg() < 1 {
		return fmt.Errorf("command required")
	}
	cmd := flags.Arg(0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := tzpro.NewClient(apiurl, nil).WithLogger(log.Log)

	switch cmd {
	case "export":
		return exportAliases(ctx, client)
	case "import":
		return importAliases(ctx, client)
	case "purge":
		return purgeAliases(ctx, client)
	case "inspect":
		return inspectAlias(ctx, client)
	case "add":
		return addAlias(ctx, client)
	case "update":
		return updateAlias(ctx, client)
	case "remove":
		return removeAlias(ctx, client)
	case "validate":
		return validateAliases(ctx, client)
	default:
		return fmt.Errorf("unkown command %s", cmd)
	}
}

func filenameArg() string {
	var name string
	if n := flags.NArg() - 1; n >= 1 {
		name = flags.Arg(n)
	}
	if strings.HasPrefix(name, "-") || strings.Contains(name, "=") {
		name = ""
	}
	return name
}

func makeFilename() string {
	name := filenameArg()
	if len(name) > 0 {
		if !strings.HasSuffix(name, ".json") {
			name += ".json"
		}
		return name
	}
	return defaultFilePrefix + "-" + time.Now().UTC().Format("2006-01-02T15-04-05") + ".json"
}

func exportAliases(ctx context.Context, c *tzpro.Client) error {
	fname := makeFilename()
	aliases, err := c.Metadata.List(ctx)
	if err != nil {
		return err
	}
	log.Debugf("Exporting %s", fname)
	f, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// change format to map
	mapped := make(map[string]index.Metadata)
	for _, v := range SortedAliases(aliases) {
		mapped[v.ID()] = v
	}

	enc := json.NewEncoder(f)
	if sorted {
		if err := enc.Encode(mapped); err != nil {
			return fmt.Errorf("%s: %v", fname, err)
		}
	} else {
		if err := enc.Encode(aliases); err != nil {
			return fmt.Errorf("%s: %v", fname, err)
		}
	}
	log.Infof("Successfully exported %d aliases to %s", len(aliases), fname)
	return nil
}

func parseAddressAndAssetId(n string) (addr tezos.Address, id *int64, err error) {
	fields := strings.Split(n, "/")
	addr, err = tezos.ParseAddress(fields[0])
	if err != nil {
		err = fmt.Errorf("%s: %v", n, err)
		return
	}
	if len(fields) > 1 {
		i, e := strconv.ParseInt(fields[1], 10, 64)
		if e != nil {
			err = fmt.Errorf("%s: %v", n, e)
			return
		}
		id = &i
	}
	return
}

func importAliases(ctx context.Context, c *tzpro.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing filename")
	}
	fname := flags.Arg(1)
	buf, err := os.ReadFile(fname)
	if err != nil {
		return fmt.Errorf("%s: %v", fname, err)
	}

	content := make(map[string]index.Metadata)
	if err := json.Unmarshal(buf, &content); err != nil {
		return fmt.Errorf("%s: %v", fname, err)
	}

	aliases, err := c.Metadata.List(ctx)
	if err != nil {
		return err
	}

	mapped := make(map[string]index.Metadata)
	for _, v := range aliases {
		mapped[v.ID()] = v
	}

	log.Infof("Importing %d aliases", len(content))
	count := 0
	ins := make([]index.Metadata, 0)
	for n, v := range content {
		v.Address, v.AssetId, err = parseAddressAndAssetId(n)
		if err != nil {
			return err
		}
		// merge existing models (imported models have priority)
		if md, ok := mapped[v.ID()]; ok {
			v = md.Merge(v)
		}
		ins = append(ins, v)
		count++
	}
	if len(ins) > 0 {
		if _, err := c.Metadata.Create(ctx, ins); err != nil {
			return err
		}
	}
	log.Infof("Successfully imported %d aliases", count)
	return nil
}

func purgeAliases(ctx context.Context, c *tzpro.Client) error {
	if !nobackup {
		log.Debugf("Creating backup")
		if err := exportAliases(ctx, c); err != nil {
			return err
		}
	}
	if err := c.Metadata.Purge(ctx); err != nil {
		return err
	}
	log.Info("Purged all aliases")
	return nil
}

func inspectAlias(ctx context.Context, c *tzpro.Client) error {
	addr, id, err := parseAddressAndAssetId(flags.Arg(1))
	if err != nil {
		return err
	}
	var alias index.Metadata
	if id == nil {
		alias, err = c.Metadata.GetWallet(ctx, addr)
	} else {
		alias, err = c.Metadata.GetAsset(ctx, tezos.NewToken(addr, tezos.NewZ(*id)))
	}
	if err != nil {
		return fmt.Errorf("%s/%d: %v", addr, id, err)
	}
	print(&alias)
	return nil
}

func addAlias(ctx context.Context, c *tzpro.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing arguments")
	}
	kvp, err := parseArgs(flags.Args()[2:])
	if err != nil {
		return err
	}

	addr, id, err := parseAddressAndAssetId(flags.Arg(1))
	if err != nil {
		return err
	}

	// load existing alias, ignore errors
	var alias index.Metadata
	if id == nil {
		alias, _ = c.Metadata.GetWallet(ctx, addr)
	} else {
		alias, _ = c.Metadata.GetAsset(ctx, tezos.NewToken(addr, tezos.NewZ(*id)))
	}

	// always set identifier
	alias.Address = addr
	alias.AssetId = id
	if err := fillStruct(kvp, &alias); err != nil {
		return err
	}
	log.Debugf("Add %#v", alias)
	_, err = c.Metadata.Create(ctx, []index.Metadata{alias})
	if err != nil {
		return err
	}
	log.Infof("Added alias %s/%d", alias.Address, alias.AssetId)
	print(&alias)
	return nil
}

func updateAlias(ctx context.Context, c *tzpro.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing arguments")
	}
	addr, id, err := parseAddressAndAssetId(flags.Arg(1))
	if err != nil {
		return err
	}
	var alias index.Metadata
	if id == nil {
		alias, err = c.Metadata.GetWallet(ctx, addr)
	} else {
		alias, err = c.Metadata.GetAsset(ctx, tezos.NewToken(addr, tezos.NewZ(*id)))
	}
	if err != nil {
		return fmt.Errorf("%s/%d: %v", addr, id, err)
	}
	kvp, err := parseArgs(flags.Args()[2:])
	if err != nil {
		return err
	}
	if err := fillStruct(kvp, &alias); err != nil {
		return err
	}
	log.Debugf("Update %#v", alias)
	if !nobackup {
		if err := exportAliases(ctx, c); err != nil {
			return err
		}
	}
	alias, err = c.Metadata.Update(ctx, alias)
	if err != nil {
		return err
	}
	log.Infof("Updated alias %s/%d", alias.Address, alias.AssetId)
	print(&alias)
	return nil
}

func removeAlias(ctx context.Context, c *tzpro.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing arguments")
	}
	if !nobackup {
		log.Debugf("Creating backup")
		if err := exportAliases(ctx, c); err != nil {
			return err
		}
	}
	addr, id, err := parseAddressAndAssetId(flags.Arg(1))
	if err != nil {
		return err
	}
	if id == nil {
		err = c.Metadata.RemoveWallet(ctx, addr)
	} else {
		err = c.Metadata.RemoveAsset(ctx, tezos.NewToken(addr, tezos.NewZ(*id)))
	}
	if err != nil {
		return err
	}
	log.Infof("Removed alias %s/%d", addr, id)
	return nil
}

type SortedAliases []index.Metadata

func (s SortedAliases) MarshalJSON() ([]byte, error) {
	sorted := make([]index.Metadata, 0, len(s))
	canSort := true
	for _, v := range s {
		sorted = append(sorted, v)
		canSort = canSort && v.Has("alias")
	}
	if canSort {
		sort.Slice(sorted, func(i, j int) bool {
			return strings.ToUpper(sorted[i].Alias().Name) < strings.ToUpper(sorted[j].Alias().Name)
		})
	}

	buf := make([]byte, 0, 1024*1024)
	buf = append(buf, '{')
	for i, v := range sorted {
		buf = strconv.AppendQuote(buf, v.Address.String())
		buf = append(buf, ':')
		if b, err := json.Marshal(v); err != nil {
			return nil, err
		} else {
			buf = append(buf, b...)
		}
		if i < len(sorted)-1 {
			buf = append(buf, ',')
		}
		buf = append(buf, '\n')
	}
	buf = append(buf, '}')

	return buf, nil
}

func validateAliases(ctx context.Context, c *tzpro.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing filename")
	}
	fname := flags.Arg(1)
	buf, err := os.ReadFile(fname)
	if err != nil {
		return fmt.Errorf("%s: %w", fname, err)
	}

	content := make(map[string]index.Metadata)
	if err := json.Unmarshal(buf, &content); err != nil {
		return fmt.Errorf("%s: %w", fname, err)
	}

	ss, err := c.Metadata.GetSchemas(ctx)
	if err != nil {
		return err
	}

	schemas := make(map[string]*jsonschema.Schema)
	for n, s := range ss {
		schema := &jsonschema.Schema{}
		if err := json.Unmarshal(s, schema); err != nil {
			return fmt.Errorf("metadata: reading %s schema failed: %v", n, err)
		}
		schemas[n] = schema
	}

	allgood := true
	for n, v := range content {
		validate := func(name string, buf []byte) bool {
			var failed bool
			if s, ok := schemas[name]; ok {
				errs, err := s.ValidateBytes(ctx, buf)
				if err != nil {
					failed = true
					fmt.Printf("Error: %s - %s: %v\n", n, name, err)
				}
				for _, e := range errs {
					failed = true
					fmt.Printf("Error: %s - %s: %v\n", n, name, e.Error())
				}
			}
			return !failed
		}
		for name, data := range v.Contents {
			buf, _ := json.Marshal(data)
			allgood = allgood && validate(name, buf)
		}
	}
	if allgood {
		fmt.Println("OK.")
	}

	return nil
}
