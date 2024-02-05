// Copyright (c) 2020-2024 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

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

	"blockwatch.cc/tzgo/tezos"
)

const (
	libraryVersion = "v18"
	userAgent      = "tzindex/v" + libraryVersion
	mediaType      = "application/json"
)

// Client manages communication with a Tezos RPC server.
type Client struct {
	// HTTP client used to communicate with the Tezos node API.
	client *http.Client
	// Base URL for API requests.
	baseURL *url.URL
	// User agent name for client.
	userAgent string
	// Optional API key for protected endpoints
	apiKey string
	// The chain the client will query.
	chainId tezos.ChainIdHash
	// The current chain configuration.
	params *Params
	// Number of retries
	numRetries int
	// Time between retries
	retryDelay time.Duration
}

// NewClient returns a new Tezos RPC client.
func NewClient(baseURL string, httpClient *http.Client) (*Client, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if !strings.HasPrefix(baseURL, "http") {
		baseURL = "http://" + baseURL
	}
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}
	q := u.Query()
	key := q.Get("X-Api-Key")
	if key != "" {
		q.Del("X-Api-Key")
		u.RawQuery = q.Encode()
	}
	c := &Client{
		client:     httpClient,
		baseURL:    u,
		userAgent:  userAgent,
		apiKey:     key,
		numRetries: 3,
		retryDelay: time.Second,
	}
	return c, nil
}

func (c *Client) Init(ctx context.Context) error {
	return c.ResolveChainConfig(ctx)
}

func (c *Client) IsInitialized() bool {
	return c.chainId.IsValid()
}

func (c *Client) Params() *Params {
	return c.params
}

func (c *Client) WithChainId(id tezos.ChainIdHash) *Client {
	c.chainId = id.Clone()
	return c
}

func (c *Client) WithParams(p *Params) *Client {
	c.params = p
	return c
}

func (c *Client) WithUserAgent(s string) *Client {
	c.userAgent = s
	return c
}

func (c *Client) WithApiKey(s string) *Client {
	c.apiKey = s
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

func (c *Client) ResolveChainConfig(ctx context.Context) error {
	id, err := c.GetChainId(ctx)
	if err != nil {
		return err
	}
	c.chainId = id
	p, err := c.GetParams(ctx, Head)
	if err != nil {
		return err
	}
	c.params = p
	return nil
}

func (c *Client) Get(ctx context.Context, urlpath string, result interface{}) error {
	req, err := c.NewRequest(ctx, http.MethodGet, urlpath, nil)
	if err != nil {
		return err
	}
	return c.Do(req, result)
}

func (c *Client) GetAsync(ctx context.Context, urlpath string, mon Monitor) error {
	req, err := c.NewRequest(ctx, http.MethodGet, urlpath, nil)
	if err != nil {
		return err
	}
	return c.DoAsync(req, mon)
}

// NewRequest creates a Tezos RPC request.
func (c *Client) NewRequest(ctx context.Context, method, urlStr string, body interface{}) (*http.Request, error) {
	rel, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	u := c.baseURL.ResolveReference(rel)

	buf := new(bytes.Buffer)
	if body != nil {
		err = json.NewEncoder(buf).Encode(body)
		if err != nil {
			return nil, err
		}
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

	log.Debug(newLogClosure(func() string {
		d, _ := httputil.DumpRequest(req, true)
		return string(d)
	}))

	return req, nil
}

func (c *Client) handleResponse(resp *http.Response, v interface{}) error {
	return json.NewDecoder(resp.Body).Decode(v)
}

func (c *Client) handleResponseMonitor(ctx context.Context, resp *http.Response, mon Monitor) {
	// decode stream
	dec := json.NewDecoder(resp.Body)

	// close body when stream stopped
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	for {
		chunkVal := mon.New()
		if err := dec.Decode(chunkVal); err != nil {
			select {
			case <-mon.Closed():
				return
			case <-ctx.Done():
				return
			default:
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				mon.Err(io.EOF)
				return
			}
			mon.Err(fmt.Errorf("rpc: %w", err))
			return
		}
		select {
		case <-mon.Closed():
			return
		case <-ctx.Done():
			return
		default:
			mon.Send(ctx, chunkVal)
		}
	}
}

// Do retrieves values from the API and marshals them into the provided interface.
func (c *Client) Do(req *http.Request, v interface{}) error {
	var (
		resp *http.Response
		err  error
	)
	for retries := c.numRetries + 1; retries > 0; retries-- {
		resp, err = c.client.Do(req)
		if err == nil && resp != nil && resp.StatusCode <= 500 {
			break
		}
		retryable := isNetError(err) || (resp != nil && resp.StatusCode > 500)
		if err != nil && !retryable {
			log.Warnf("rpc: %T %v", err, err)
			return err
		}
		if resp != nil && retries > 1 {
			resp.Body.Close()
			resp = nil
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

	log.Trace(newLogClosure(func() string {
		d, _ := httputil.DumpResponse(resp, true)
		return string(d)
	}))

	statusClass := resp.StatusCode / 100
	if statusClass == 2 {
		if v == nil {
			return nil
		}
		err = c.handleResponse(resp, v)
		if err != nil {
			return err
		}
		mustClear = false
		return nil
	}

	mustClear = false
	return handleError(resp)
}

// DoAsync retrieves values from the API and sends responses using the provided monitor.
func (c *Client) DoAsync(req *http.Request, mon Monitor) error {
	//nolint:bodyclose
	resp, err := c.client.Do(req)
	if err != nil {
		if e, ok := err.(*url.Error); ok {
			return e.Err
		}
		return err
	}

	if resp.StatusCode == http.StatusNoContent {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return nil
	}

	statusClass := resp.StatusCode / 100
	if statusClass == 2 {
		if mon != nil {
			go func() {
				c.handleResponseMonitor(req.Context(), resp, mon)
			}()
			return nil
		}
	} else {
		return handleError(resp)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return nil
}

func handleError(resp *http.Response) error {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	httpErr := httpError{
		request:    resp.Request.Method + " " + resp.Request.URL.RequestURI(),
		status:     resp.Status,
		statusCode: resp.StatusCode,
		body:       bytes.ReplaceAll(body, []byte("\n"), []byte{}),
	}

	if resp.StatusCode < 500 || !strings.Contains(resp.Header.Get("Content-Type"), "application/json") {
		// Other errors with unknown body format (usually human readable string)
		return &httpErr
	}

	if body[0] == '[' {
		// try decode node errors
		errs := NodeErrors{}
		if err := json.Unmarshal(body, &errs); err == nil {
			return &rpcError{
				httpError: &httpErr,
				errors:    errs.Interface(),
			}
		} else {
			log.Errorf("rpc: error decoding RPC error: %v", err)
		}
	}
	// proxy and gateway errors will be returned as httpError
	return &httpErr
}
