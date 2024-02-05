// Copyright (c) 2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/echa/config"
	"github.com/echa/log"
)

const (
	userAgent     = "tzindex/v18.0"
	jsonMediaType = "application/json"
)

// Client manages communication with a Tezos RPC server.
type Client struct {
	// HTTP client used to communicate with the Tezos node API.
	client *http.Client
	// log is a private log instance
	log log.Logger
	// Base URL for API requests.
	base *url.URL
	// User agent name for client.
	userAgent string
	// Number of retries
	numRetries int
	// Time between retries
	retryDelay time.Duration
	// Api key
	apiKey string
}

func New() *Client {
	return (&Client{
		client:    http.DefaultClient,
		log:       log.Log,
		userAgent: userAgent,
	}).
		WithRetry(
			config.GetInt("meta.http.max_retries"),
			config.GetDuration("meta.http.retry_delay"),
		).
		WithApiKey(config.GetString("meta.http.api_key"))
}

func (c *Client) Connect(base string) error {
	if base != "" {
		if !strings.HasPrefix(base, "http") {
			base = "http://" + base
		}
		u, err := url.Parse(base)
		if err != nil {
			return err
		}
		c.base = u
	}
	return nil
}

func (c *Client) WithLogger(log log.Logger) *Client {
	c.log = log
	return c
}

func (c *Client) SetLogLevel(lvl log.Level) {
	c.log.SetLevel(lvl)
}

func (c *Client) WithHttpClient(hc *http.Client) *Client {
	c.client = hc
	return c
}

func (c *Client) WithApiKey(k string) *Client {
	c.apiKey = k
	return c
}

func (c *Client) WithUserAgent(s string) *Client {
	c.userAgent = s
	return c
}

func (c *Client) WithRetry(num int, delay time.Duration) *Client {
	c.numRetries = num
	if num < 0 {
		c.numRetries = int(^uint(0)>>1) - 1 // max int - 1
	}
	c.retryDelay = delay
	return c
}

func (c *Client) Get(ctx context.Context, urlpath string) ([]byte, error) {
	req, err := c.NewRequest(ctx, http.MethodGet, urlpath, jsonMediaType, nil)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	err = c.Do(req, c.NewRawResponseDecoder(buf))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (c *Client) GetJson(ctx context.Context, urlpath string, result any) error {
	req, err := c.NewRequest(ctx, http.MethodGet, urlpath, jsonMediaType, nil)
	if err != nil {
		return err
	}
	return c.Do(req, c.NewJsonResponseDecoder(result))
}

func (c *Client) PostJson(ctx context.Context, urlpath string, body, result any) error {
	req, err := c.NewRequest(ctx, http.MethodPost, urlpath, jsonMediaType, body)
	if err != nil {
		return err
	}
	return c.Do(req, c.NewJsonResponseDecoder(result))
}

func (c *Client) NewRequest(ctx context.Context, method, urlStr, mediaType string, body any) (*http.Request, error) {
	rel, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if rel.Host == "" {
		return nil, fmt.Errorf("%w: missing host in url", ErrPermanent)
	}

	u := rel
	if c.base != nil {
		u = c.base.ResolveReference(rel)
	}

	var buf *bytes.Buffer
	if body != nil {
		if b, ok := body.([]byte); ok {
			buf = bytes.NewBuffer(b)
		} else {
			buf = new(bytes.Buffer)
			err = json.NewEncoder(buf).Encode(body)
			if err != nil {
				return nil, err
			}
		}
	} else {
		buf = new(bytes.Buffer)
	}

	req, err := http.NewRequest(method, u.String(), buf)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	req.Header.Add("Content-Type", mediaType)
	req.Header.Add("Accept", mediaType)
	req.Header.Add("User-Agent", c.userAgent)
	if c.apiKey != "" {
		req.Header.Add("X-Api-Key", c.apiKey)
	}

	c.log.Debugf("%s %s %s", req.Method, req.URL, req.Proto)
	c.log.Trace(log.NewClosure(func() string {
		d, _ := httputil.DumpRequest(req, true)
		return string(d)
	}))

	return req, nil
}

type ResponseHandler func(context.Context, *http.Response) error

func (c *Client) NewJsonResponseDecoder(v any) ResponseHandler {
	return func(_ context.Context, resp *http.Response) error {
		if v == nil {
			return nil
		}
		return json.NewDecoder(resp.Body).Decode(v)
	}
}

func (c *Client) NewRawResponseDecoder(v *bytes.Buffer) ResponseHandler {
	return func(_ context.Context, resp *http.Response) error {
		if v == nil {
			v = new(bytes.Buffer)
		}
		_, err := io.Copy(v, resp.Body)
		return err
	}
}

// Do retrieves values from the API and marshals them into the provided interface.
func (c *Client) Do(req *http.Request, fn ResponseHandler) error {
	var (
		resp *http.Response
		err  error
	)
	for retries := c.numRetries + 1; retries > 0; retries-- {
		resp, err = c.client.Do(req)
		if err == nil {
			if !IsRetriableStatusCode(resp.StatusCode) {
				break
			}
		}
		if !IsNetError(err) {
			return err
		}
		select {
		case <-req.Context().Done():
			return req.Context().Err()
		case <-time.After(c.retryDelay):
			// continue
		}
	}
	if err != nil {
		return err
	}

	mustClear := true
	defer func() {
		if mustClear {
			_, _ = io.Copy(io.Discard, resp.Body)
		}
		resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	log.Trace(log.NewClosure(func() string {
		d, _ := httputil.DumpResponse(resp, true)
		return string(d)
	}))

	statusClass := resp.StatusCode / 100
	if statusClass == 2 {
		err = fn(req.Context(), resp)
		if err != nil {
			return err
		}
		mustClear = false
		return nil
	}

	mustClear = false
	return handleError(resp)
}

func handleError(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	return &httpError{
		request:    resp.Request.Method + " " + resp.Request.URL.String(),
		status:     resp.Status,
		statusCode: resp.StatusCode,
		body:       body,
	}
}
