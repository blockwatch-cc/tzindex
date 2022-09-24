// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020-2021 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"encoding/json"
	"fmt"
)

const (
	// ErrorKindPermanent Tezos RPC error kind.
	ErrorKindPermanent = "permanent"
	// ErrorKindTemporary Tezos RPC error kind.
	ErrorKindTemporary = "temporary"
	// ErrorKindBranch Tezos RPC error kind.
	ErrorKindBranch = "branch"
)

func ErrorStatus(err error) int {
	switch e := err.(type) {
	case *httpError:
		return e.statusCode
	default:
		return 0
	}
}

// Error is a Tezos error as documented on http://tezos.gitlab.io/mainnet/api/errors.html.
type Error interface {
	error
	ErrorID() string
	ErrorKind() string
}

// GenericError is a basic error type
type GenericError struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

func (e *GenericError) Error() string {
	return fmt.Sprintf("tezos: kind = %q, id = %q", e.Kind, e.ID)
}

// ErrorID returns Tezos error id
func (e *GenericError) ErrorID() string {
	return e.ID
}

// ErrorKind returns Tezos error kind
func (e *GenericError) ErrorKind() string {
	return e.Kind
}

// HTTPStatus interface represents an unprocessed HTTP reply
type HTTPStatus interface {
	Request() string // e.g. GET /...
	Status() string  // e.g. "200 OK"
	StatusCode() int // e.g. 200
	Body() []byte
}

// HTTPError retains HTTP status
type HTTPError interface {
	error
	HTTPStatus
}

// RPCError is a Tezos RPC error as documented on http://tezos.gitlab.io/mainnet/api/errors.html.
type RPCError interface {
	Error
	HTTPStatus
	Errors() []Error // returns all errors as a slice
}

// Errors is a slice of Error with custom JSON unmarshaller
type Errors []Error

// UnmarshalJSON implements json.Unmarshaler
func (e *Errors) UnmarshalJSON(data []byte) error {
	var errs []*GenericError

	if err := json.Unmarshal(data, &errs); err != nil {
		return err
	}

	*e = make(Errors, len(errs))
	for i, g := range errs {
		// TODO: handle different kinds
		(*e)[i] = g
	}

	return nil
}

func (e Errors) Error() string {
	if len(e) == 0 {
		return ""
	}
	return e[0].Error()
}

// ErrorID returns Tezos error id
func (e Errors) ErrorID() string {
	if len(e) == 0 {
		return ""
	}
	return e[0].ErrorID()
}

// ErrorKind returns Tezos error kind
func (e Errors) ErrorKind() string {
	if len(e) == 0 {
		return ""
	}
	return e[0].ErrorKind()
}

type httpError struct {
	request    string
	status     string
	statusCode int
	body       []byte
}

func (e *httpError) Error() string {
	return fmt.Sprintf("rpc: %s status %d (%v)", e.request, e.statusCode, string(e.body))
}

func (e *httpError) Request() string {
	return e.request
}

func (e *httpError) Status() string {
	return e.status
}

func (e *httpError) StatusCode() int {
	return e.statusCode
}

func (e *httpError) Body() []byte {
	return e.body
}

type rpcError struct {
	*httpError
	errors Errors
}

func (e *rpcError) Error() string {
	return e.errors.Error()
}

func (e *rpcError) ErrorID() string {
	return e.errors.ErrorID()
}

func (e *rpcError) ErrorKind() string {
	return e.errors.ErrorKind()
}

func (e *rpcError) Errors() []Error {
	return e.errors
}

type plainError struct {
	*httpError
	msg string
}

func (e *plainError) Error() string {
	return e.msg
}

var (
	_ Error    = &GenericError{}
	_ Error    = Errors{}
	_ RPCError = &rpcError{}
)
