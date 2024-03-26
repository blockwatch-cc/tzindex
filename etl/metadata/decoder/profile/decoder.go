// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package profile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"blockwatch.cc/tzindex/etl/metadata"
	"blockwatch.cc/tzindex/etl/model"
	"blockwatch.cc/tzindex/etl/task"
	"github.com/echa/config"
	"github.com/tidwall/gjson"
)

const (
	TzProfileV0 uint64 = 0x30794422432f1557
	TzProfileV1 uint64 = 0x60ad2404b71de830
)

func init() {
	metadata.RegisterDecoder(TzProfileV0, NewDecoder)
	metadata.RegisterDecoder(TzProfileV1, NewDecoder)
}

type Decoder struct {
	baseUrl string
}

func NewDecoder(_ uint64) metadata.Decoder {
	return &Decoder{
		baseUrl: config.GetString("meta.kepler.url"),
	}
}

func (d *Decoder) Namespace() string {
	return metadata.NsProfile
}

func (d *Decoder) OnOperation(_ context.Context, op *model.Op) ([]*metadata.Event, error) {
	switch op.Type {
	case model.OpTypeOrigination:
		// detect new profiles
		return d.decodeDeployEvents(op)
	case model.OpTypeTransaction:
		// detect claim updates
		return d.decodeCallEvents(op)
	default:
		return nil, nil
	}
}

func (d *Decoder) decodeDeployEvents(op *model.Op) ([]*metadata.Event, error) {
	store, err := d.DecodeStorage(op.Storage)
	if err != nil {
		return nil, err
	}
	var events []*metadata.Event
	if len(store.Claims) > 0 {
		for _, c := range store.Claims {
			u, err := d.toKeplerPath(c.Url)
			if err != nil {
				return nil, err
			}
			events = append(events, &metadata.Event{
				Owner: store.Owner,
				Type:  metadata.EventTypeResolve,
				Flags: 1, // = add/update after completion
				Data:  u,
			})
		}
	}
	return events, nil
}

func (d *Decoder) decodeCallEvents(op *model.Op) ([]*metadata.Event, error) {
	store, err := d.DecodeStorage(op.Storage)
	if err != nil {
		return nil, err
	}
	args, err := d.DecodeParams(op.Parameters)
	if err != nil {
		return nil, err
	}

	var events []*metadata.Event
	if args.IsUpdate {
		// schedule new claims
		for _, c := range args.Claims {
			u, err := d.toKeplerPath(c.Url)
			if err != nil {
				return nil, err
			}
			events = append(events, &metadata.Event{
				Owner: store.Owner,
				Type:  metadata.EventTypeResolve,
				Flags: 1, // = add/update after completion
				Data:  u,
			})
		}
	} else {
		// schedule revoked claims
		for _, c := range args.Claims {
			u, err := d.toKeplerPath(c.Url)
			if err != nil {
				return nil, err
			}
			events = append(events, &metadata.Event{
				Owner: store.Owner,
				Type:  metadata.EventTypeResolve,
				Flags: 0, // = remove after completion
				Data:  u,
			})
		}
	}
	return events, nil
}

// process resolved claims
// flag == 1: update from claim, on error do nothing
// flag == 0: remove from claim, on error remove all
func (d *Decoder) OnTaskComplete(ctx context.Context, meta *model.Metadata, res *task.TaskResult) ([]*metadata.Event, error) {
	switch res.Status {
	case task.TaskStatusIdle, task.TaskStatusRunning:
		return nil, nil
	case task.TaskStatusFailed, task.TaskStatusTimeout:
		// update request, do nothing on error
		if res.Flags == 1 {
			return nil, nil
		}
		// remove request, remove all on error
		return []*metadata.Event{{
			Owner: res.Owner,
			Type:  metadata.EventTypeRemove,
		}}, nil
	case task.TaskStatusSuccess:
		// continue below
	}

	// decode current metadata
	var p metadata.TezosProfile
	contents := struct {
		Profile *metadata.TezosProfile `json:"tzprofile"`
	}{
		Profile: &p,
	}
	if len(meta.Content) > 0 {
		if err := json.Unmarshal(meta.Content, &contents); err != nil {
			return nil, err
		}
	}

	// decode claim
	if res.Flags == 1 {
		// apply claim
		cred := gjson.ParseBytes(res.Data)
		subj := cred.Get("credentialSubject")
		evdc := cred.Get("evidence")
		for _, t := range cred.Get("type").Array() {
			switch t.String() {
			case "BasicProfile":
				p.Alias = subj.Get("alias").String()
				p.Description = subj.Get("description").String()
				p.Logo = subj.Get("logo").String()
				p.Website = subj.Get("website").String()
			case "DnsVerification":
				p.DomainName = strings.TrimPrefix(subj.Get("sameAs").String(), "dns:")
			case "TwitterVerification":
				p.Twitter = evdc.Get("handle").String()
			case "EthereumAddressControl":
				p.Ethereum = subj.Get("address").String()
			case "EthereumControl":
				p.Ethereum = subj.Get("wallet").String()
			case "GitHubVerification":
				p.Github = evdc.Get("handle").String()
			case "DiscordVerification":
				p.Discord = evdc.Get("handle").String()
			}
		}
		p.Serial++
		return []*metadata.Event{{
			Owner: res.Owner,
			Type:  metadata.EventTypeUpdate,
			Data:  &p,
		}}, nil

	} else {
		// revoke claim
		var m map[string]any
		if err := json.Unmarshal(res.Data, &m); err != nil {
			return nil, err
		}
		for _, t := range m["type"].([]any) {
			switch t.(string) {
			case "BasicProfile":
				p.Alias = ""
				p.Description = ""
				p.Logo = ""
				p.Website = ""
			case "TwitterVerification":
				p.Twitter = ""
			case "EthereumAddressControl":
				p.Ethereum = ""
			case "EthereumControl":
				p.Ethereum = ""
			case "GitHubVerification":
				p.Github = ""
			case "DnsVerification":
				p.DomainName = ""
			case "DiscordVerification":
				p.Discord = ""
			}
		}
		p.Serial++
		return []*metadata.Event{{
			Owner: res.Owner,
			Type:  metadata.EventTypeUpdate,
			Data:  &p,
		}}, nil
	}
}

func (d *Decoder) toKeplerPath(path string) (string, error) {
	url, ok := strings.CutPrefix(path, "kepler://")
	if !ok {
		return "", fmt.Errorf("invalid kepler url format %q", path)
	}
	url = strings.TrimPrefix(url, "v0:")
	url = strings.Join([]string{d.baseUrl, url}, "/")
	return url, nil
}
