// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"blockwatch.cc/tzgo/tezos"
	"blockwatch.cc/tzstats-go"
	"github.com/daviddengcn/go-colortext"
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
			fmt.Printf("  payout.from           ([]string) list of related baker addresses, only for kind payout\n")
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
			os.Exit(0)
		}
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		log.SetLevel(log.LevelDebug)
		tzstats.UseLogger(log.Log)
	}

	if err := run(); err != nil {
		if e, ok := tzstats.IsApiError(err); ok {
			fmt.Printf("Error: %s: %s\n", e.Errors[0].Message, e.Errors[0].Detail)
		} else {
			fmt.Printf("Error: %v\n", err)
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

	client, err := tzstats.NewClient(apiurl, nil)
	if err != nil {
		return err
	}

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

func exportAliases(ctx context.Context, c *tzstats.Client) error {
	fname := makeFilename()
	aliases, err := c.ListMetadata(ctx)
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
	mapped := make(map[string]tzstats.Metadata)
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

func importAliases(ctx context.Context, c *tzstats.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing filename")
	}
	fname := flags.Arg(1)
	buf, err := ioutil.ReadFile(fname)
	if err != nil {
		return fmt.Errorf("%s: %v", fname, err)
	}

	content := make(map[string]tzstats.Metadata)
	if err := json.Unmarshal(buf, &content); err != nil {
		return fmt.Errorf("%s: %v", fname, err)
	}

	aliases, err := c.ListMetadata(ctx)
	if err != nil {
		return err
	}

	mapped := make(map[string]tzstats.Metadata)
	for _, v := range aliases {
		mapped[v.ID()] = v
	}

	log.Infof("Importing %d aliases", len(content))
	count := 0
	ins := make([]tzstats.Metadata, 0)
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
		if _, err := c.CreateMetadata(ctx, ins); err != nil {
			return err
		}
	}
	log.Infof("Successfully imported %d aliases", count)
	return nil
}

func purgeAliases(ctx context.Context, c *tzstats.Client) error {
	if !nobackup {
		log.Debugf("Creating backup")
		if err := exportAliases(ctx, c); err != nil {
			return err
		}
	}
	if err := c.PurgeMetadata(ctx); err != nil {
		return err
	}
	log.Info("Purged all aliases")
	return nil
}

func inspectAlias(ctx context.Context, c *tzstats.Client) error {
	addr, id, err := parseAddressAndAssetId(flags.Arg(1))
	if err != nil {
		return err
	}
	var alias tzstats.Metadata
	if id == nil {
		alias, err = c.GetAccountMetadata(ctx, addr)
	} else {
		alias, err = c.GetAssetMetadata(ctx, addr, *id)
	}
	if err != nil {
		return fmt.Errorf("%s/%d: %v", addr, id, err)
	}
	print(&alias)
	return nil
}

func addAlias(ctx context.Context, c *tzstats.Client) error {
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
	var alias tzstats.Metadata
	if id == nil {
		alias, _ = c.GetAccountMetadata(ctx, addr)
	} else {
		alias, _ = c.GetAssetMetadata(ctx, addr, *id)
	}

	// always set identifier
	alias.Address = addr
	alias.AssetId = id
	if err := fillStruct(kvp, &alias); err != nil {
		return err
	}
	log.Debugf("Add %#v", alias)
	_, err = c.CreateMetadata(ctx, []tzstats.Metadata{alias})
	if err != nil {
		return err
	}
	log.Infof("Added alias %s/%d", alias.Address, alias.AssetId)
	print(&alias)
	return nil
}

func updateAlias(ctx context.Context, c *tzstats.Client) error {
	if flags.NArg() < 2 {
		return fmt.Errorf("missing arguments")
	}
	addr, id, err := parseAddressAndAssetId(flags.Arg(1))
	if err != nil {
		return err
	}
	var alias tzstats.Metadata
	if id == nil {
		alias, err = c.GetAccountMetadata(ctx, addr)
	} else {
		alias, err = c.GetAssetMetadata(ctx, addr, *id)
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
	alias, err = c.UpdateMetadata(ctx, alias)
	if err != nil {
		return err
	}
	log.Infof("Updated alias %s/%d", alias.Address, alias.AssetId)
	print(&alias)
	return nil
}

func removeAlias(ctx context.Context, c *tzstats.Client) error {
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
		err = c.RemoveAccountMetadata(ctx, addr)
	} else {
		err = c.RemoveAssetMetadata(ctx, addr, *id)
	}
	if err != nil {
		return err
	}
	log.Infof("Removed alias %s/%d", addr, id)
	return nil
}

type kvpairs map[string]string

func parseArgs(args []string) (kvpairs, error) {
	kvp := make(kvpairs, 0)
	for _, arg := range args {
		ok, k, v := parseKeyValue(arg)
		if !ok {
			return nil, fmt.Errorf("bad key/value: %s", arg)
		}
		kvp[k] = v
	}
	return kvp, nil
}

func unescape(s string) string {
	u := make([]rune, 0, len(s))
	var escape bool
	for _, c := range s {
		if escape {
			u = append(u, c)
			escape = false
			continue
		}
		if c == '\\' {
			escape = true
			continue
		}
		u = append(u, c)
	}

	return string(u)
}

func parseKeyValue(keyvalue string) (bool, string, string) {
	k := make([]rune, 0, len(keyvalue))
	var escape bool
	for i, c := range keyvalue {
		if escape {
			k = append(k, c)
			escape = false
			continue
		}
		if c == '\\' {
			escape = true
			continue
		}
		if c == '=' {
			return true, string(k), unescape(keyvalue[i+1:])
		}
		k = append(k, c)
	}

	return false, "", ""
}

func print(val interface{}) {
	body, _ := json.MarshalIndent(val, "", "    ")
	if nocolor {
		os.Stdout.Write(body)
	} else {
		raw := make(map[string]interface{})
		dec := json.NewDecoder(bytes.NewBuffer(body))
		dec.UseNumber()
		dec.Decode(&raw)
		printJSON(1, raw, false)
	}
}

func printJSON(depth int, val interface{}, isKey bool) {
	switch v := val.(type) {
	case nil:
		ct.ChangeColor(ct.Blue, false, ct.None, false)
		fmt.Print("null")
		ct.ResetColor()
	case bool:
		ct.ChangeColor(ct.Blue, false, ct.None, false)
		if v {
			fmt.Print("true")
		} else {
			fmt.Print("false")
		}
		ct.ResetColor()
	case string:
		if isKey {
			ct.ChangeColor(ct.Blue, true, ct.None, false)
		} else {
			ct.ChangeColor(ct.Yellow, false, ct.None, false)
		}
		fmt.Print(strconv.Quote(v))
		ct.ResetColor()
	case json.Number:
		ct.ChangeColor(ct.Blue, false, ct.None, false)
		fmt.Print(v)
		ct.ResetColor()
	case map[string]interface{}:

		if len(v) == 0 {
			fmt.Print("{}")
			break
		}

		var keys []string

		for h := range v {
			keys = append(keys, h)
		}

		sort.Strings(keys)

		fmt.Println("{")
		needNL := false
		for _, key := range keys {
			if needNL {
				fmt.Print(",\n")
			}
			needNL = true
			for i := 0; i < depth; i++ {
				fmt.Print("    ")
			}

			printJSON(depth+1, key, true)
			fmt.Print(": ")
			printJSON(depth+1, v[key], false)
		}
		fmt.Println("")

		for i := 0; i < depth-1; i++ {
			fmt.Print("    ")
		}
		fmt.Print("}")

	case []interface{}:

		if len(v) == 0 {
			fmt.Print("[]")
			break
		}

		fmt.Println("[")
		needNL := false
		for _, e := range v {
			if needNL {
				fmt.Print(",\n")
			}
			needNL = true
			for i := 0; i < depth; i++ {
				fmt.Print("    ")
			}

			printJSON(depth+1, e, false)
		}
		fmt.Println("")

		for i := 0; i < depth-1; i++ {
			fmt.Print("    ")
		}
		fmt.Print("]")
	default:
		fmt.Println("unknown type:", reflect.TypeOf(v))
	}
}

var stringSliceType = reflect.TypeOf([]string(nil))

func setField(dst interface{}, name, value string) error {
	structValue := reflect.ValueOf(dst).Elem()
	typ := structValue.Type()
	index := -1
	for i := 0; i < typ.NumField(); i++ {
		tag := strings.Split(typ.Field(i).Tag.Get("json"), ",")[0]
		if tag == "-" {
			continue
		}
		if tag == name {
			index = i
			break
		}
	}
	if index < 0 {
		return fmt.Errorf("No such field: %s in struct", name)
	}

	structFieldValue := structValue.FieldByIndex([]int{index})
	if !structFieldValue.IsValid() {
		return fmt.Errorf("No such field: %s in struct", name)
	}

	if !structFieldValue.CanSet() {
		return fmt.Errorf("Cannot set %s field value", name)
	}

	switch structFieldValue.Kind() {
	case reflect.Int, reflect.Int64:
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		structFieldValue.SetInt(i)
	case reflect.Int32:
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return err
		}
		structFieldValue.SetInt(i)
	case reflect.Int16:
		i, err := strconv.ParseInt(value, 10, 16)
		if err != nil {
			return err
		}
		structFieldValue.SetInt(i)
	case reflect.Int8:
		i, err := strconv.ParseInt(value, 10, 8)
		if err != nil {
			return err
		}
		structFieldValue.SetInt(i)
	case reflect.Uint, reflect.Uint64:
		i, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		structFieldValue.SetUint(i)
	case reflect.Uint32:
		i, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return err
		}
		structFieldValue.SetUint(i)
	case reflect.Uint16:
		i, err := strconv.ParseUint(value, 10, 16)
		if err != nil {
			return err
		}
		structFieldValue.SetUint(i)
	case reflect.Uint8:
		i, err := strconv.ParseUint(value, 10, 8)
		if err != nil {
			return err
		}
		structFieldValue.SetUint(i)
	case reflect.Float64:
		i, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return err
		}
		structFieldValue.SetFloat(i)
	case reflect.Float32:
		i, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return err
		}
		structFieldValue.SetFloat(i)
	case reflect.Bool:
		b, err := strconv.ParseBool(value)
		if err != nil {
			return err
		}
		structFieldValue.SetBool(b)
	case reflect.String:
		structFieldValue.SetString(value)
	case reflect.Slice:
		if structFieldValue.Type() == stringSliceType {
			structFieldValue.Set(reflect.ValueOf(strings.Split(value, ",")))
		} else {
			return fmt.Errorf("Unsupported slice type %s", structFieldValue.Type())
		}
	default:
		return fmt.Errorf("Unsupported struct type %s", structFieldValue.Type())
	}
	return nil
}

// we assume exactly two nesting levels and add all detected schemas
// as schema descriptors to Extra
func fillStruct(m map[string]string, alias *tzstats.Metadata) error {
	for k, v := range m {
		keyParts := strings.Split(k, ".")
		ns := keyParts[0]
		var err error
		switch ns {
		case "alias":
			if alias.Alias == nil {
				alias.Alias = &tzstats.AliasMetadata{}
			}
			err = setField(alias.Alias, keyParts[1], v)
		case "baker":
			if alias.Baker == nil {
				alias.Baker = &tzstats.BakerMetadata{}
			}
			err = setField(alias.Baker, keyParts[1], v)
		case "payout":
			if alias.Payout == nil {
				alias.Payout = &tzstats.PayoutMetadata{}
			}
			err = setField(alias.Payout, keyParts[1], v)
		case "asset":
			if alias.Asset == nil {
				alias.Asset = &tzstats.AssetMetadata{}
			}
			err = setField(alias.Asset, keyParts[1], v)
		case "location":
			if alias.Location == nil {
				alias.Location = &tzstats.LocationMetadata{}
			}
			err = setField(alias.Location, keyParts[1], v)
		case "domain":
			if alias.Domain == nil {
				alias.Domain = &tzstats.DomainMetadata{}
			}
			err = setField(alias.Domain, keyParts[1], v)
		case "media":
			if alias.Media == nil {
				alias.Media = &tzstats.MediaMetadata{}
			}
			err = setField(alias.Media, keyParts[1], v)
		case "rights":
			if alias.Rights == nil {
				alias.Rights = &tzstats.RightsMetadata{}
			}
			err = setField(alias.Rights, keyParts[1], v)
		case "social":
			if alias.Social == nil {
				alias.Social = &tzstats.SocialMetadata{}
			}
			err = setField(alias.Social, keyParts[1], v)
		case "tz16":
			if alias.Tz16 == nil {
				alias.Tz16 = &tzstats.Tz16Metadata{}
			}
			err = setField(alias.Tz16, keyParts[1], v)
		case "tz21":
			if alias.Tz21 == nil {
				alias.Tz21 = &tzstats.Tz21Metadata{}
			}
			err = setField(alias.Tz21, keyParts[1], v)
		default:
			if alias.Extra == nil {
				alias.Extra = make(map[string]interface{})
			}
			// use custom metadata schema
			desc, ok := alias.Extra[ns]
			if !ok {
				desc = make(map[string]interface{})
			}
			// check value type
			if descVal, ok := desc.(map[string]interface{}); ok {
				descVal[keyParts[1]] = v
			} else {
				if err := setField(desc, keyParts[1], v); err != nil {
					return err
				}
			}
			alias.Extra[ns] = desc
		}
		if err != nil {
			return fmt.Errorf("Setting %s=%s: %v", k, v, err)
		}
	}
	return nil
}

type SortedAliases []tzstats.Metadata

func (s SortedAliases) MarshalJSON() ([]byte, error) {
	sorted := make([]tzstats.Metadata, 0, len(s))
	canSort := true
	for _, v := range s {
		sorted = append(sorted, v)
		canSort = canSort && v.Alias != nil
	}
	if canSort {
		sort.Slice(sorted, func(i, j int) bool {
			return strings.ToUpper(sorted[i].Alias.Name) < strings.ToUpper(sorted[j].Alias.Name)
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
