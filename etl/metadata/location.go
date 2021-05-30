// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"strings"

	"github.com/echa/code/iata"
	"github.com/echa/code/iso"
)

func init() {
	var b strings.Builder
	for i, v := range iso.ISO_3166_1_COUNTRY_CODES {
		if i > 0 {
			b.WriteRune(',')
		}
		b.WriteRune('"')
		b.WriteString(v)
		b.WriteRune('"')
	}
	str := strings.Replace(locationSchema, `{{COUNTRY_CODES}}`, b.String(), -1)
	b.Reset()
	for i, v := range iata.IATA_LARGE_AIRPORT_CODES {
		if i > 0 {
			b.WriteRune(',')
		}
		b.WriteRune('"')
		b.WriteString(v.String())
		b.WriteRune('"')
	}
	str = strings.Replace(str, `{{CITY_CODES}}`, b.String(), -1)
	LoadSchema(locationNs, []byte(str), &Location{})
}

const (
	locationNs     = "location"
	locationSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzstats.com/metadata/schemas/location.json",
	"title": "Location Info",
    "description": "A geographical location and coordinates.",
	"type": "object",
	"properties": {
		"lat": {
		  "type": "number",
		  "minimum": -90,
		  "maximum": 90,
		  "description": "GPS Latitude"
		},
		"lon": {
		  "type": "number",
		  "minimum": -180,
		  "maximum": 180,
		  "description": "GPS Longitude"
		},
		"alt": {
		  "type": "number",
		  "description": "GPS Altitude"
		},
		"country": {
		  "type": "string",
		  "description": "ISO 3166-1 Alpha-2 Country Code",
		  "enum": [{{COUNTRY_CODES}}]
		},
		"city": {
		  "type": "string",
		  "description": "IATA Airport code",
		  "enum": [{{CITY_CODES}}]
		}
	}
}`
)

type Location struct {
	Country   iso.Country      `json:"country,omitempty"`
	City      iata.AirportCode `json:"city,omitempty"`
	Latitude  float64          `json:"lon,omitempty"`
	Longitude float64          `json:"lat,omitempty"`
	Altitude  float64          `json:"alt,omitempty"`
}

func (d Location) Namespace() string {
	return locationNs
}

func (d Location) Validate() error {
	s, ok := GetSchema(locationNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
