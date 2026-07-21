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

// ErrReadTimeout is returned when a linearizable read could not be confirmed
// against a majority within the deadline — typically because this leader has
// been partitioned away from its cluster.
//
// It is a refusal, not a failure to find data. Answering from local state
// instead would be exactly the stale read the confirmation exists to prevent, so
// the only honest response is to decline and let the client retry elsewhere.
var ErrReadTimeout = errors.New("read timeout: could not confirm leadership with a quorum")

// ErrTooStale is returned for a bounded-staleness read when this node cannot
// prove it has been in contact with a leader recently enough to satisfy the
// client's MaxStaleness.
//
// It is a refusal, and deliberately not a fallback. A client that named a bound
// is entitled to have it enforced; answering anyway would make the bound
// decorative.
var ErrTooStale = errors.New("read too stale: this node has not confirmed currency with a leader recently enough")

// ErrMaxStalenessRequired is returned when a bounded read arrives without a
// bound. A client asking for possibly-stale data must say how stale it will
// tolerate, rather than inheriting a number the server picked.
var ErrMaxStalenessRequired = errors.New("bounded reads require a positive max staleness")
