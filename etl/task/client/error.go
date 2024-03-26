// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
)

var (
	ErrTimeout   = &errTimeout{errors.New("timeout error")}
	ErrNetwork   = &errNetwork{errors.New("network error")}
	ErrPermanent = &errPermanent{errors.New("permanent error")}
)

type ErrorWrapper interface {
	Wrap(error) error
}

func WrapError(e error, t ErrorWrapper) error {
	return t.Wrap(e)
}

// permanent errors
type errPermanent struct {
	err error
}

func (e errPermanent) Error() string {
	return fmt.Sprintf("permanent: %v", e.err)
}

func (e errPermanent) Unwrap() error {
	return e.err
}

func (e errPermanent) Is(target error) bool {
	_, ok := target.(*errPermanent)
	return ok
}

func (e errPermanent) Wrap(target error) error {
	return &errPermanent{
		err: target,
	}
}

// timeout errors
type errTimeout struct {
	err error
}

func (e errTimeout) Error() string {
	return fmt.Sprintf("timeout: %v", e.err)
}

func (e errTimeout) Unwrap() error {
	return e.err
}

func (e errTimeout) Is(target error) bool {
	_, ok := target.(*errTimeout)
	return ok
}

func (e errTimeout) Wrap(target error) error {
	return &errTimeout{
		err: target,
	}
}

// network errors
type errNetwork struct {
	err error
}

func (e errNetwork) Error() string {
	return fmt.Sprintf("network: %v", e.err)
}

func (e errNetwork) Unwrap() error {
	return e.err
}

func (e errNetwork) Is(target error) bool {
	_, ok := target.(*errNetwork)
	return ok
}

func (e errNetwork) Wrap(target error) error {
	return &errNetwork{
		err: target,
	}
}

// HTTP errors
func ErrorStatus(err error) int {
	if e, ok := err.(*httpError); ok {
		return e.statusCode
	}
	var httpErr *httpError
	if errors.As(err, &httpErr) {
		return httpErr.statusCode
	}
	return 0
}

func IsHttpError(err error) bool {
	_, ok := err.(HTTPError)
	return ok
}

func IsRetriableStatusCode(c int) bool {
	switch c {
	case 404, 429, 500, 502, 503, 504, 522, 524:
		return true
	default:
		return false
	}
}

func IsRetriableHttpError(err error) bool {
	return IsRetriableStatusCode(ErrorStatus(err))
}

func IsNetError(err error) bool {
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

func WrapNetError(err error) error {
	if err == nil {
		return nil
	}
	// don't wrap already wrapped errors twice
	if errors.Is(err, ErrPermanent) ||
		errors.Is(err, ErrTimeout) ||
		errors.Is(err, ErrNetwork) {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return err
	case IsNetError(err):
		switch e := err.(type) {
		case *net.OpError:
			if e.Timeout() {
				return WrapError(err, ErrTimeout)
			}
			return WrapError(err, ErrNetwork)
		case *url.Error:
			if e.Timeout() {
				return WrapError(err, ErrTimeout)
			}
			if errors.Is(err, io.EOF) {
				return WrapError(err, ErrPermanent)
			}
			errstr := err.Error()
			switch {
			case strings.Contains(errstr, "x509:"):
				// invalid/unreachable http urls may produce this
				return WrapError(err, ErrPermanent)
			case strings.Contains(errstr, "http: no Host in request URL"):
				// bad URL (e.g. using http:///)
				return WrapError(err, ErrPermanent)
			case strings.Contains(errstr, "no such host"):
				// bad URL (e.g. invalid host)
				return WrapError(err, ErrPermanent)
			case strings.Contains(errstr, "connect: connection refused"):
				// host is down, let's retry
				return WrapError(err, ErrTimeout)
			case strings.Contains(errstr, "server misbehaving"):
				// DNS resolution SERVERFAIL, let's retry
				return WrapError(err, ErrTimeout)
			}
			return WrapError(err, ErrPermanent)
		}
	case IsHttpError(err):
		// classify HTTP errors received from upstream APIs
		switch s := ErrorStatus(err); s {
		case 429, 503, 504, 522, 524:
			return WrapError(err, ErrTimeout)
		case 502:
			return WrapError(err, ErrNetwork)
		case 400, 401, 403, 404:
			return WrapError(err, ErrPermanent)
		default:
			if s >= 500 {
				return WrapError(err, ErrPermanent)
			}
			return WrapError(err, ErrNetwork)
		}
	}
	// fallback, classify as network error so user may retry
	return WrapError(fmt.Errorf("unhandled %T: %w", err, err), ErrNetwork)
}

// HTTPStatus interface represents an unprocessed HTTP reply
type HTTPStatus interface {
	Request() string // e.g. GET /...
	Status() string  // e.g. "200 OK"
	StatusCode() int // e.g. 200
}

// HTTPError retains HTTP status and error response body
type HTTPError interface {
	error
	HTTPStatus
	Body() []byte
}

type httpError struct {
	request    string
	status     string
	statusCode int
	body       []byte
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s: %s", e.request, e.status)
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

func (e *httpError) DecodeError(v interface{}) error {
	return json.Unmarshal(e.body, v)
}

var (
	_ HTTPError = &httpError{}
)
