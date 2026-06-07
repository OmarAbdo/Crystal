package engine

import "errors"

// ErrNotLeader is returned when a proposal is submitted to a non-leader node.
// The HTTP handler translates this into a 403 with the current leader ID.
var ErrNotLeader = errors.New("not the leader")

// ErrCommitTimeout is returned when quorum was not reached within the deadline.
var ErrCommitTimeout = errors.New("commit timeout: lost quorum or cluster degraded")
