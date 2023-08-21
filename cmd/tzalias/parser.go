// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package main

import (
	"encoding"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"blockwatch.cc/tzpro-go/tzpro/index"
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
func fillStruct(m map[string]string, alias *index.Metadata) error {
	for k, v := range m {
		keyParts := strings.Split(k, ".")
		ns := keyParts[0]
		data := alias.Get(ns)
		var err error
		if len(keyParts) > 1 {
			// check value type
			if dataMap, ok := data.(map[string]interface{}); ok {
				dataMap[keyParts[1]] = v
			} else {
				err = setField(data, keyParts[1], v)
			}
			alias.Set(ns, data)
		} else if v == "null" {
			alias.Delete(ns)
		}
		if err != nil {
			return fmt.Errorf("Setting %s=%s: %w", k, v, err)
		}
	}
	return nil
}
