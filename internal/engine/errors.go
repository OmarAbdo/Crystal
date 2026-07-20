package engine

import "errors"

// ErrNotLeader is returned when a proposal is submitted to a non-leader node, or
// when a pending proposal is abandoned because this node stopped being leader.
// The HTTP handler translates it into 421 Misdirected Request, with the leader's
// address in X-Raft-Leader when one is known.
var ErrNotLeader = errors.New("not the leader")

// ErrCommitTimeout is returned when quorum was not reached within the deadline.
var ErrCommitTimeout = errors.New("commit timeout: lost quorum or cluster degraded")

// errUnreachable stands in for a transport failure in tests that never intend
// to exercise the network.
var errUnreachable = errors.New("peer unreachable")
