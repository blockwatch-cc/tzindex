// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
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

// NodeError is a basic error type
type NodeError struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("tezos: kind = %q, id = %q", e.Kind, e.ID)
}

// ErrorID returns Tezos error id
func (e *NodeError) ErrorID() string {
	return e.ID
}

// ErrorKind returns Tezos error kind
func (e *NodeError) ErrorKind() string {
	return e.Kind
}

type NodeErrors []NodeError

func (n NodeErrors) Interface() []Error {
	if n == nil {
		return nil
	}
	e := make([]Error, len(n))
	for i := range n {
		e[i] = &n[i]
	}
	return e
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

var (
	_ Error    = &NodeError{}
	_ Error    = Errors{}
	_ RPCError = &rpcError{}
)

func isNetError(err error) bool {
	if err == nil {
		return false
	}
	// direct type
	switch err.(type) {
	case *net.OpError:
		return true
	case *net.DNSError:
		return true
	case *os.SyscallError:
		return true
	case *url.Error:
		return true
	}
	// wrapped
	var (
		neterr *net.OpError
		dnserr *net.DNSError
		oserr  *os.SyscallError
		urlerr *url.Error
	)
	switch {
	case errors.As(err, &neterr):
		return true
	case errors.As(err, &dnserr):
		return true
	case errors.As(err, &oserr):
		return true
	case errors.As(err, &urlerr):
		return true
	}
	return false
}
