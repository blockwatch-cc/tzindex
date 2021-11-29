// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"blockwatch.cc/tzstats-go"
)

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

var (
	stringSliceType     = reflect.TypeOf([]string(nil))
	textUnmarshalerType = reflect.TypeOf((*encoding.TextUnmarshaler)(nil)).Elem()
)

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
		if structFieldValue.CanInterface() && structFieldValue.Type().Implements(textUnmarshalerType) {
			if err := structFieldValue.Interface().(encoding.TextUnmarshaler).UnmarshalText([]byte(value)); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("Unsupported struct type %s", structFieldValue.Type())
		}
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
			if len(keyParts) > 1 {
				err = setField(alias.Alias, keyParts[1], v)
			} else if v == "null" {
				alias.Alias = nil
			}
		case "baker":
			if alias.Baker == nil {
				alias.Baker = &tzstats.BakerMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Baker, keyParts[1], v)
			} else if v == "null" {
				alias.Baker = nil
			}
		case "payout":
			if alias.Payout == nil {
				alias.Payout = &tzstats.PayoutMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Payout, keyParts[1], v)
			} else if v == "null" {
				alias.Payout = nil
			}
		case "asset":
			if alias.Asset == nil {
				alias.Asset = &tzstats.AssetMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Asset, keyParts[1], v)
			} else if v == "null" {
				alias.Asset = nil
			}
		case "location":
			if alias.Location == nil {
				alias.Location = &tzstats.LocationMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Location, keyParts[1], v)
			} else if v == "null" {
				alias.Location = nil
			}
		case "domain":
			if alias.Domain == nil {
				alias.Domain = &tzstats.DomainMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Domain, keyParts[1], v)
			} else if v == "null" {
				alias.Domain = nil
			}
		case "media":
			if alias.Media == nil {
				alias.Media = &tzstats.MediaMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Media, keyParts[1], v)
			} else if v == "null" {
				alias.Media = nil
			}
		case "rights":
			if alias.Rights == nil {
				alias.Rights = &tzstats.RightsMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Rights, keyParts[1], v)
			} else if v == "null" {
				alias.Rights = nil
			}
		case "social":
			if alias.Social == nil {
				alias.Social = &tzstats.SocialMetadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Social, keyParts[1], v)
			} else if v == "null" {
				alias.Social = nil
			}
		case "tz16":
			if alias.Tz16 == nil {
				alias.Tz16 = &tzstats.Tz16Metadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Tz16, keyParts[1], v)
			} else if v == "null" {
				alias.Tz16 = nil
			}
		case "tz21":
			if alias.Tz21 == nil {
				alias.Tz21 = &tzstats.Tz21Metadata{}
			}
			if len(keyParts) > 1 {
				err = setField(alias.Tz21, keyParts[1], v)
			} else if v == "null" {
				alias.Tz21 = nil
			}
		case "updated":
			if alias.Updated == nil {
				alias.Updated = &tzstats.UpdatedMetadata{}
			}
			err = setField(alias.Updated, keyParts[1], v)
		default:
			if alias.Extra == nil {
				alias.Extra = make(map[string]interface{})
			}
			// use custom metadata schema
			desc, ok := alias.Extra[ns]
			if !ok {
				desc = make(map[string]interface{})
			}
			if len(keyParts) > 1 {
				// check value type
				if descVal, ok := desc.(map[string]interface{}); ok {
					descVal[keyParts[1]] = v
				} else {
					if err := setField(desc, keyParts[1], v); err != nil {
						return err
					}
				}
				alias.Extra[ns] = desc
			} else if v == "null" {
				delete(alias.Extra, ns)
			}
		}
		if err != nil {
			return fmt.Errorf("Setting %s=%s: %w", k, v, err)
		}
	}
	return nil
}
