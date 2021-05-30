// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

func init() {
	LoadSchema(mediaNs, []byte(mediaSchema), &Media{})
}

const (
	mediaNs     = "media"
	mediaSchema = `{
	"$schema": "http://json-schema.org/draft/2019-09/schema#",
	"$id": "https://api.tzstats.com/metadata/schemas/media.json",
	"title": "Media Info",
    "description": "A list of media related settings.",
	"type": "object",
	"properties": {
		"thumbnail_uri": {
		  "type": "string",
	      "format": "uri",
    	  "pattern": "^(https?|ipfs)://"
  		},
		"artifact_uri": {
		  "type": "string",
	      "format": "uri",
    	  "pattern": "^(https?|ipfs)://"
  		},
		"format": {
		  "type": "string",
		  "format": "regex",
		  "pattern": "^(application|image|video|audio|text)/"
		},
		"language": {
		  "type": "string"
		}
	}
}`
)

type Media struct {
	ThumbnailUri string `json:"thumbnail_uri,omitempty"`
	ArtifactUri  string `json:"artifact_uri,omitempty"`
	Format       string `json:"format,omitempty"`
	Language     string `json:"language,omitempty"`
}

func (d Media) Namespace() string {
	return mediaNs
}

func (d Media) Validate() error {
	s, ok := GetSchema(mediaNs)
	if ok {
		return s.Validate(d)
	}
	return nil
}
