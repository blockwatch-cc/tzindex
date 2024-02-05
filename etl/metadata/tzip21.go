// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

import (
	"time"

	"blockwatch.cc/tzgo/tezos"
)

func init() {
	LoadSchema(tz21Ns, []byte(tz21Schema), &Tz21{})
}

// https://gitlab.com/tzip/tzip/-/blob/master/proposals/tzip-21/metadata-schema.json
const (
	tz21Ns     = "tz21"
	tz21Schema = `{
  "$schema": "http://json-schema.org/draft/2019-09/schema#",
  "$id": "https://api.tzpro.io/metadata/schemas/tz21.json",
  "title": "Rich Metadata",
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "description": {
      "type": "string",
      "description": "General notes, abstracts, or summaries about the contents of an asset."
    },
    "minter": {
      "type": "string",
      "format": "tzaddress",
      "description": "The tz address responsible for minting the asset."
    },
    "creators": {
      "type": "array",
      "description": "The primary person, people, or organization(s) responsible for creating the intellectual content of the asset.",
      "uniqueItems": true,
      "items": {
        "type": "string"
      }
    },
    "contributors": {
      "type": "array",
      "description": "The person, people, or organization(s) that have made substantial creative contributions to the asset.",
      "uniqueItems": true,
      "items": {
        "type": "string"
      }
    },
    "publishers": {
      "type": "array",
      "description": "The person, people, or organization(s) primarily responsible for distributing or making the asset available to others in its present form.",
      "uniqueItems": true,
      "items": {
        "type": "string"
      }
    },
    "date": {
      "type": "string",
      "format": "date-time",
      "description": "A date associated with the creation or availability of the asset."
    },
    "blockLevel": {
      "type": "integer",
      "description": "Chain block level associated with the creation or availability of the asset."
    },
    "type": {
      "type": "string",
      "description": "A broad definition of the type of content of the asset."
    },
    "tags": {
      "type": "array",
      "description": "A list of tags that describe the subject or content of the asset.",
      "uniqueItems": true,
      "items": {
        "type": "string"
      }
    },
    "genres": {
      "type": "array",
      "description": "A list of genres that describe the subject or content of the asset.",
      "uniqueItems": true,
      "items": {
        "type": "string"
      }
    },
    "language": {
      "type": "string",
      "format": "https://tools.ietf.org/html/rfc1766",
      "description": "The language of the intellectual content of the asset as defined in RFC 1776."
    },
    "identifier": {
      "type": "string",
      "description": "A string or number used to uniquely identify the asset. Ex. URL, URN, UUID, ISBN, etc."
    },
    "rights": {
      "type": "string",
      "description": "A statement about the asset rights."
    },
    "rightUri": {
      "type": "string",
      "format": "uri-reference",
      "description": "Links to a statement of rights."
    },
    "artifactUri": {
      "type": "string",
      "format": "uri-reference",
      "description": "A URI to the asset."
    },
    "displayUri": {
      "type": "string",
      "format": "uri-reference",
      "description": "A URI to an image of the asset. Used for display purposes."
    },
    "thumbnailUri": {
      "type": "string",
      "format": "uri-reference",
      "description": "A URI to an image of the asset for wallets and client applications to have a scaled down image to present to end-users. Reccomened maximum size of 350x350px."
    },
    "externalUri": {
      "type": "string",
      "format": "uri-reference",
      "description": "A URI with additional information about the subject or content of the asset."
    },
    "isTransferable": {
      "type": "boolean",
      "description": "All tokens will be transferable by default to allow end-users to send them to other end-users. However, this field exists to serve in special cases where owners will not be able to transfer the token."
    },
    "isBooleanAmount": {
      "type": "boolean",
      "description": "Describes whether an account can have an amount of exactly 0 or 1. (The purpose of this field is for wallets to determine whether or not to display balance information and an amount field when transferring.)"
    },
    "shouldPreferSymbol": {
      "type": "boolean",
      "description": "Allows wallets to decide whether or not a symbol should be displayed in place of a name."
    },
    "formats": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/format"
      }
    },
    "attributes": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/attribute"
      },
      "description": "Custom attributes about the subject or content of the asset."
    },
    "assets": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/asset"
      },
      "description": "Facilitates the description of collections and other types of resources that contain multiple assets."
    }
  },
  "$defs": {
    "format": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "uri": {
          "type": "string",
          "format": "uri-reference",
          "description": "A URI to the asset represented in this format."
        },
        "hash": {
          "type": "string",
          "description": "A checksum hash of the content of the asset in this format."
        },
        "mimeType": {
          "type": "string",
          "description": "Media (MIME) type of the format."
        },
        "fileSize": {
          "type": "integer",
          "description": "Size in bytes of the content of the asset in this format."
        },
        "fileName": {
          "type": "string",
          "description": "Filename for the asset in this format. For display purposes."
        },
        "duration": {
          "type": "string",
          "format": "time",
          "description": "Time duration of the content of the asset in this format."
        },
        "dimensions": {
          "$ref": "#/$defs/dimensions",
          "description": "Dimensions of the content of the asset in this format."
        },
        "dataRate": {
          "$ref": "#/$defs/dataRate",
          "description": "Data rate which the content of the asset in this format was captured at."
        }
      }
    },
    "attribute": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "name": {
          "type": "string",
          "description": "Name of the attribute."
        },
        "value": {
          "type": "string",
          "description": "Value of the attribute."
        },
        "type": {
          "type": "string",
          "description": "Type of the value. To be used for display purposes."
        }
      },
      "required": [
        "name",
        "value"
      ]
    },
    "dataRate": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "value": {
          "type": "integer"
        },
        "unit": {
          "type": "string"
        }
      },
      "required": [
        "unit",
        "value"
      ]
    },
    "dimensions": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "value": {
          "type": "string"
        },
        "unit": {
          "type": "string"
        }
      },
      "required": [
        "unit",
        "value"
      ]
    }
  }
}`
)

type Tz21 struct {
	Tz21Asset
}

type Tz21Asset struct {
	Description        string          `json:"description"`
	Minter             tezos.Address   `json:"minter"`
	Creators           []string        `json:"creators"`
	Contributors       []string        `json:"contributors"`
	Publishers         []string        `json:"publishers"`
	Date               time.Time       `json:"date"`
	BlockLevel         int64           `json:"blockLevel"`
	Type               string          `json:"type"`
	Tags               []string        `json:"tags"`
	Genres             []string        `json:"genres"`
	Language           string          `json:"language"`
	Identifier         string          `json:"identifier"`
	Rights             string          `json:"rights"`
	RightUri           string          `json:"rightUri"`
	ArtifactUri        string          `json:"artifactUri"`
	DisplayUri         string          `json:"displayUri"`
	ThumbnailUri       string          `json:"thumbnailUri"`
	ExternalUri        string          `json:"externalUri"`
	IsTransferable     bool            `json:"isTransferable"`
	IsBooleanAmount    bool            `json:"isBooleanAmount"`
	ShouldPreferSymbol bool            `json:"shouldPreferSymbol"`
	Formats            []Tz21Format    `json:"formats"`
	Attributes         []Tz21Attribute `json:"attributes"`
	Assets             []Tz21Asset     `json:"assets"`
}

type Tz21Format struct {
	Uri        string        `json:"uri"`
	Hash       string        `json:"hash"`
	MimeType   string        `json:"mimeType"`
	FileSize   int64         `json:"fileSize"`
	FileName   string        `json:"fileName"`
	Duration   string        `json:"duration"`
	Dimensions Tz21Dimension `json:"dimensions"`
	DataRate   Tz21DataRate  `json:"dataRate"`
}

type Tz21Attribute struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Type  string `json:"type,omitempty"`
}

type Tz21Dimension struct {
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

type Tz21DataRate struct {
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

func (d Tz21) Namespace() string {
	return tz21Ns
}

func (d Tz21) Validate() error {
	s, ok := GetSchema(tz21Ns)
	if ok {
		return s.Validate(d)
	}
	return nil
}
