// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package metadata

type Schema interface {
	// Returns the schema's unique namespace which is used as value prefix.
	Namespace() string

	// Validates JSON encoded data against built-in schema and returns
	// an error when validation fails.
	ValidateBytes([]byte) error

	// Validates a Go type against built-in schema and returns an error when
	// validation fails.
	Validate(interface{}) error

	// Creates a schema-compatible descriptor as Go type
	NewDescriptor() Descriptor
}

type Descriptor interface {
	// Returns the schema's unique namespace which is used as value prefix.
	Namespace() string

	// Validates descriptor against built-in schema and returns an error when
	// validation fails.
	Validate() error
}
