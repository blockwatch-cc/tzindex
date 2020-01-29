// Copyright (c) 2018 ECAD Labs Inc. MIT License
// Copyright (c) 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package rpc

import (
	"context"
	"fmt"
	"net/url"
	"time"
)

// NetworkStats models global network bandwidth totals and usage in B/s.
type NetworkStats struct {
	TotalBytesSent int64 `json:"total_sent,string"`
	TotalBytesRecv int64 `json:"total_recv,string"`
	CurrentInflow  int64 `json:"current_inflow"`
	CurrentOutflow int64 `json:"current_outflow"`
}

// NetworkConnection models detailed information for one network connection.
type NetworkConnection struct {
	Incoming         bool              `json:"incoming"`
	PeerID           string            `json:"peer_id"`
	IDPoint          NetworkAddress    `json:"id_point"`
	RemoteSocketPort uint16            `json:"remote_socket_port"`
	Versions         []*NetworkVersion `json:"versions"`
	Private          bool              `json:"private"`
	LocalMetadata    NetworkMetadata   `json:"local_metadata"`
	RemoteMetadata   NetworkMetadata   `json:"remote_metadata"`
}

// NetworkAddress models a point's address and port.
type NetworkAddress struct {
	Addr string `json:"addr"`
	Port uint16 `json:"port"`
}

// NetworkVersion models a network-layer version of a node.
type NetworkVersion struct {
	Name  string `json:"name"`
	Major uint16 `json:"major"`
	Minor uint16 `json:"minor"`
}

// NetworkMetadata models metadata of a node.
type NetworkMetadata struct {
	DisableMempool bool `json:"disable_mempool"`
	PrivateNode    bool `json:"private_node"`
}

// NetworkConnectionTimestamp represents peer address with timestamp added
type NetworkConnectionTimestamp struct {
	NetworkAddress
	Timestamp time.Time
}

// UnmarshalJSON implements json.Unmarshaler
func (n *NetworkConnectionTimestamp) UnmarshalJSON(data []byte) error {
	return unmarshalNamedJSONArray(data, &n.NetworkAddress, &n.Timestamp)
}

// NetworkPeer represents peer info
type NetworkPeer struct {
	PeerID                    string                      `json:"-"`
	Score                     int64                       `json:"score"`
	Trusted                   bool                        `json:"trusted"`
	ConnMetadata              *NetworkMetadata            `json:"conn_metadata"`
	State                     string                      `json:"state"`
	ReachableAt               *NetworkAddress             `json:"reachable_at"`
	Stat                      NetworkStats                `json:"stat"`
	LastEstablishedConnection *NetworkConnectionTimestamp `json:"last_established_connection"`
	LastSeen                  *NetworkConnectionTimestamp `json:"last_seen"`
	LastFailedConnection      *NetworkConnectionTimestamp `json:"last_failed_connection"`
	LastRejectedConnection    *NetworkConnectionTimestamp `json:"last_rejected_connection"`
	LastDisconnection         *NetworkConnectionTimestamp `json:"last_disconnection"`
	LastMiss                  *NetworkConnectionTimestamp `json:"last_miss"`
}

// networkPeerWithID is a heterogeneously encoded NetworkPeer with ID as a first array member
// See OperationAlt for details
type networkPeerWithID NetworkPeer

func (n *networkPeerWithID) UnmarshalJSON(data []byte) error {
	return unmarshalNamedJSONArray(data, &n.PeerID, (*NetworkPeer)(n))
}

// NetworkPoint represents network point info
type NetworkPoint struct {
	Address                   string            `json:"-"`
	Trusted                   bool              `json:"trusted"`
	GreylistedUntil           time.Time         `json:"greylisted_until"`
	State                     NetworkPointState `json:"state"`
	P2PPeerID                 string            `json:"p2p_peer_id"`
	LastFailedConnection      time.Time         `json:"last_failed_connection"`
	LastRejectedConnection    *IDTimestamp      `json:"last_rejected_connection"`
	LastEstablishedConnection *IDTimestamp      `json:"last_established_connection"`
	LastDisconnection         *IDTimestamp      `json:"last_disconnection"`
	LastSeen                  *IDTimestamp      `json:"last_seen"`
	LastMiss                  time.Time         `json:"last_miss"`
}

// networkPointAlt is a heterogeneously encoded NetworkPoint with address as a first array member
// See OperationAlt for details
type networkPointAlt NetworkPoint

func (n *networkPointAlt) UnmarshalJSON(data []byte) error {
	return unmarshalNamedJSONArray(data, &n.Address, (*NetworkPoint)(n))
}

// NetworkPointState represents point state
type NetworkPointState struct {
	EventKind string `json:"event_kind"`
	P2PPeerID string `json:"p2p_peer_id"`
}

// IDTimestamp represents peer id with timestamp
type IDTimestamp struct {
	ID        string
	Timestamp time.Time
}

// UnmarshalJSON implements json.Unmarshaler
func (i *IDTimestamp) UnmarshalJSON(data []byte) error {
	return unmarshalNamedJSONArray(data, &i.ID, &i.Timestamp)
}

// GetNetworkStats returns current network stats https://tezos.gitlab.io/betanet/api/rpc.html#get-network-stat
func (c *Client) GetNetworkStats(ctx context.Context) (*NetworkStats, error) {
	var stats NetworkStats
	if err := c.Get(ctx, "network/stat", &stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

// GetNetworkConnections returns all network connections http://tezos.gitlab.io/mainnet/api/rpc.html#get-network-connections
func (c *Client) GetNetworkConnections(ctx context.Context) ([]*NetworkConnection, error) {
	var conns []*NetworkConnection
	if err := c.Get(ctx, "network/connections", &conns); err != nil {
		return nil, err
	}
	return conns, nil
}

// GetNetworkPeers returns the list the peers the node ever met.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers
func (c *Client) GetNetworkPeers(ctx context.Context, filter string) ([]*NetworkPeer, error) {
	u := url.URL{
		Path: "network/peers",
	}

	if filter != "" {
		q := url.Values{
			"filter": []string{filter},
		}
		u.RawQuery = q.Encode()
	}

	var peers []*networkPeerWithID
	if err := c.Get(ctx, u.String(), &peers); err != nil {
		return nil, err
	}

	ret := make([]*NetworkPeer, len(peers))
	for i, p := range peers {
		ret[i] = (*NetworkPeer)(p)
	}

	return ret, nil
}

// GetNetworkPeer returns details about a given peer.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers-peer-id
func (c *Client) GetNetworkPeer(ctx context.Context, peerID string) (*NetworkPeer, error) {
	var peer NetworkPeer
	if err := c.Get(ctx, "network/peers/"+peerID, &peer); err != nil {
		return nil, err
	}
	peer.PeerID = peerID

	return &peer, nil
}

// BanNetworkPeer blacklists the given peer.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers-peer-id-ban
func (c *Client) BanNetworkPeer(ctx context.Context, peerID string) error {
	return c.Get(ctx, "network/peers/"+peerID+"/ban", nil)
}

// TrustNetworkPeer used to trust a given peer permanently: the peer cannot be blocked (but its host IP still can).
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers-peer-id-trust
func (c *Client) TrustNetworkPeer(ctx context.Context, peerID string) error {
	return c.Get(ctx, "network/peers/"+peerID+"/trust", nil)
}

// GetNetworkPeerBanned checks if a given peer is blacklisted or greylisted.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers-peer-id-banned
func (c *Client) GetNetworkPeerBanned(ctx context.Context, peerID string) (bool, error) {
	var banned bool
	if err := c.Get(ctx, "network/peers/"+peerID+"/banned", &banned); err != nil {
		return false, err
	}
	return banned, nil
}

// GetNetworkPeerLog monitors network events related to a given peer.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers-peer-id-log
func (c *Client) GetNetworkPeerLog(ctx context.Context, peerID string) ([]*NetworkPeerLogEntry, error) {
	var log []*NetworkPeerLogEntry
	if err := c.Get(ctx, "network/peers/"+peerID+"/log", &log); err != nil {
		return nil, err
	}
	return log, nil
}

// GetNetworkPoints returns list the pool of known `IP:port` used for establishing P2P connections.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-points
func (c *Client) GetNetworkPoints(ctx context.Context, filter string) ([]*NetworkPoint, error) {
	u := url.URL{
		Path: "network/points",
	}

	if filter != "" {
		q := url.Values{
			"filter": []string{filter},
		}
		u.RawQuery = q.Encode()
	}

	var points []*networkPointAlt
	if err := c.Get(ctx, u.String(), &points); err != nil {
		return nil, err
	}

	ret := make([]*NetworkPoint, len(points))
	for i, p := range points {
		ret[i] = (*NetworkPoint)(p)
	}

	return ret, nil
}

// GetNetworkPoint returns details about a given `IP:addr`.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-points-point
func (c *Client) GetNetworkPoint(ctx context.Context, address string) (*NetworkPoint, error) {
	var point NetworkPoint
	if err := c.Get(ctx, "network/points/"+address, &point); err != nil {
		return nil, err
	}
	point.Address = address
	return &point, nil
}

// ConnectToNetworkPoint used to connect to a peer.
// https://tezos.gitlab.io/mainnet/api/rpc.html#put-network-points-point
func (c *Client) ConnectToNetworkPoint(ctx context.Context, address string, timeout time.Duration) error {
	u := url.URL{
		Path: "network/points/" + address,
	}

	if timeout > 0 {
		q := url.Values{
			"timeout": []string{fmt.Sprintf("%f", float64(timeout)/float64(time.Second))},
		}
		u.RawQuery = q.Encode()
	}

	return c.Put(ctx, u.String(), &struct{}{}, nil)
}

// BanNetworkPoint blacklists the given address.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-points-point-ban
func (c *Client) BanNetworkPoint(ctx context.Context, address string) error {
	return c.Get(ctx, "network/points/"+address+"/ban", nil)
}

// TrustNetworkPoint used to trust a given address permanently. Connections from this address can still be closed on authentication if the peer is blacklisted or greylisted.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-points-point-trust
func (c *Client) TrustNetworkPoint(ctx context.Context, address string) error {
	return c.Get(ctx, "network/points/"+address+"/trust", nil)
}

// GetNetworkPointBanned check is a given address is blacklisted or greylisted.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-points-point-banned
func (c *Client) GetNetworkPointBanned(ctx context.Context, address string) (bool, error) {
	var banned bool
	if err := c.Get(ctx, "network/points/"+address+"/banned", &banned); err != nil {
		return false, err
	}
	return banned, nil
}

// GetNetworkPointLog monitors network events related to an `IP:addr`.
// https://tezos.gitlab.io/mainnet/api/rpc.html#get-network-peers-peer-id-log
func (c *Client) GetNetworkPointLog(ctx context.Context, address string) ([]*NetworkPointLogEntry, error) {
	var log []*NetworkPointLogEntry
	if err := c.Get(ctx, "network/points/"+address+"/log", &log); err != nil {
		return nil, err
	}
	return log, nil
}
