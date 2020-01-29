// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// Error Codes

// 10xx - HTTP errors
const (
	EC_NO_ROUTE = 1000 + iota
	EC_CONTENTTYPE_UNSUPPORTED
	EC_CONTENTTYPE_UNEXPECTED
	EC_MARSHAL_FAILED
	EC_DEMARSHAL_FAILED
	EC_BAD_URL_QUERY
	EC_PARAM_REQUIRED
	EC_PARAM_INVALID
	EC_PARAM_NOISOLANG
	EC_PARAM_NOISOCOUNTRY
	EC_PARAM_NOTEXPECTED
)

// 11xx - internal server error codes
const (
	EC_DATABASE = 1100 + iota
	EC_SERVER
	EC_RPC
	EC_NETWORK
)

// 12xx - Access errors
const (
	EC_ACCESS_HEADER_MISSING = 1200 + iota
	EC_ACCESS_APIKEY_MISSING
	EC_ACCESS_APIKEY_INVALID
	EC_ACCESS_APIKEY_INACTIVE
	EC_ACCESS_APIVERSION_INVALID
	EC_ACCESS_METHOD_UNSUPPORTED
	EC_ACCESS_ORIGIN_DENIED
	EC_ACCESS_TOKEN_MISSING
	EC_ACCESS_TOKEN_MALFORMED
	EC_ACCESS_TOKEN_INVALID
	EC_ACCESS_TOKEN_REVOKED
	EC_ACCESS_TOKEN_EXPIRED
	EC_ACCESS_SCOPES_INSUFFICIENT
	EC_ACCESS_RATE_LIMITED
	EC_ACCESS_READONLY
	EC_ACCESS_DENIED
)

// 13xx - Resource errors
const (
	EC_RESOURCE_ID_MISSING = 1300 + iota
	EC_RESOURCE_ID_MALFORMED
	EC_RESOURCE_VERSION_MISMATCH
	EC_RESOURCE_TYPE_MISMATCH
	EC_RESOURCE_CONFLICT
	EC_RESOURCE_NOTFOUND
	EC_RESOURCE_EXISTS
	EC_RESOURCE_CREATE_FAILED
	EC_RESOURCE_UPDATE_FAILED
	EC_RESOURCE_DELETE_FAILED
	EC_RESOURCE_STATE_UNEXPECTED
)

type Error struct {
	Code      int    `json:"code"`
	Status    int    `json:"status"`
	Message   string `json:"message"`
	Scope     string `json:"scope"`
	Detail    string `json:"detail"`
	RequestId string `json:"request_id,omitempty"`
	Cause     error  `json:"-"`
	Reason    string `json:"reason,omitempty"`
}

type ErrorList []*Error

type ErrorResponse struct {
	Errors ErrorList `json:"errors"`
}

type ErrorWrapper func(code int, detail string, err error) error

func NewWrappedError(status int, msg string) ErrorWrapper {
	e := &Error{Status: status, Message: msg}
	return e.Complete
}

func (e *Error) Complete(code int, detail string, err error) error {
	x := &Error{
		Code:    code,
		Status:  e.Status,
		Message: e.Message,
		Scope:   e.Scope,
		Detail:  detail,
		Cause:   err,
	}
	if err != nil {
		x.Reason = err.Error()
	}
	return x
}

type ApiErrorWrapper func(code int, detail string, err error) *Error

func NewWrappedApiError(status int, msg string) ApiErrorWrapper {
	e := &Error{Status: status, Message: msg}
	return e.CompleteApiError
}

func (e *Error) CompleteApiError(code int, detail string, err error) *Error {
	x := &Error{
		Code:    code,
		Status:  e.Status,
		Message: e.Message,
		Scope:   e.Scope,
		Detail:  detail,
		Cause:   err,
	}
	if err != nil {
		x.Reason = err.Error()
	}
	return x
}

func (e *Error) String() string {
	return fmt.Sprintf("%s %s: %s", e.Scope, e.Message, e.Detail)
}

func (e *Error) Error() string {
	s := make([]string, 0)
	if e.Status != 0 {
		s = append(s, strings.Join([]string{"status", strconv.Itoa(e.Status)}, "="))
	}
	if e.Code != 0 {
		s = append(s, strings.Join([]string{"code", strconv.Itoa(e.Code)}, "="))
	}
	if e.Scope != "" {
		s = append(s, strings.Join([]string{"scope", e.Scope}, "="))
	}
	s = append(s, strings.Join([]string{"message", e.Message}, "="))
	if e.Detail != "" {
		s = append(s, strings.Join([]string{"detail", e.Detail}, "="))
	}
	if e.RequestId != "" {
		s = append(s, strings.Join([]string{"request-id", e.RequestId}, "="))
	}
	if e.Cause != nil {
		s = append(s, strings.Join([]string{"cause", e.Cause.Error()}, "="))
	}
	return strings.Join(s, " ")
}

func (e *Error) SetScope(s string) *Error {
	if e.Scope != "" {
		e.Scope = strings.Join([]string{s, e.Scope}, ": ")
	} else {
		e.Scope = s
	}
	return e
}

func (e *Error) MarshalIndent() []byte {
	errResp := ErrorResponse{
		Errors: ErrorList{e},
	}
	b, _ := json.MarshalIndent(errResp, "", "  ")
	return b
}

func (e *Error) Marshal() []byte {
	errResp := ErrorResponse{
		Errors: ErrorList{e},
	}
	b, _ := json.Marshal(errResp)
	return b
}

func ParseErrorFromStream(i io.Reader, status int) error {
	var response ErrorResponse
	jsonDecoder := json.NewDecoder(i)
	if err := jsonDecoder.Decode(&response); err != nil {
		return &Error{
			Status:  status,
			Code:    EC_DEMARSHAL_FAILED,
			Message: "parsing error response failed",
			Scope:   "ParseErrorFromStream",
			Cause:   err,
		}
	}
	return response.Errors[0]
}

func ParseErrorFromByte(b []byte, status int) error {
	var response ErrorResponse
	if len(b) > 0 {
		if err := json.Unmarshal(b, &response); err != nil {
			return &Error{
				Status:  status,
				Code:    EC_DEMARSHAL_FAILED,
				Message: "parsing error response failed",
				Scope:   "ParseErrorFromByte",
				Cause:   err,
			}
		}
	}
	return response.Errors[0]
}

func MapError(scope, msg string, err error) error {
	apiErr, ok := err.(*Error)
	if !ok {
		return EInternal(1, strings.Join([]string{scope, msg}, ":"), err)
	}
	return &Error{
		Code:    apiErr.Code,
		Status:  apiErr.Status,
		Message: msg,
		Scope:   fmt.Sprintf("%s: %s", scope, apiErr.Scope),
		Detail:  fmt.Sprintf("%s: %s", apiErr.Message, apiErr.Detail),
		Cause:   apiErr.Cause,
	}
}

// Server Error Reasons
var (
	EBadRequest         = NewWrappedError(http.StatusBadRequest, "incorrect request syntax")
	EUnauthorized       = NewWrappedError(http.StatusUnauthorized, "authentication required")
	EForbidden          = NewWrappedError(http.StatusForbidden, "access forbidden")
	ENotFound           = NewWrappedError(http.StatusNotFound, "resource not found")
	ENotAllowed         = NewWrappedError(http.StatusMethodNotAllowed, "method not allowed")
	ENotAcceptable      = NewWrappedError(http.StatusNotAcceptable, "unsupported response type")
	EBadMimetype        = NewWrappedError(http.StatusUnsupportedMediaType, "unsupported media type")
	EConflict           = NewWrappedError(http.StatusConflict, "resource state conflict")
	EInternal           = NewWrappedError(http.StatusInternalServerError, "internal server error")
	ERequestTooLarge    = NewWrappedError(http.StatusRequestEntityTooLarge, "request size exceeds our limits")
	ETooManyRequests    = NewWrappedError(http.StatusTooManyRequests, "request limit exceeded")
	EServiceUnavailable = NewWrappedError(http.StatusServiceUnavailable, "service temporarily unavailable")
	ENotImplemented     = NewWrappedError(http.StatusNotImplemented, "not implemented")
	EConnectionClosed   = NewWrappedError(499, "connection closed")
)
