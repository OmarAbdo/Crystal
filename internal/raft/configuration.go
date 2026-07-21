package raft

// Configuration is the set of servers participating in consensus, and the thing
// every quorum decision is made against (§6).
//
// Until now the cluster was a fixed integer — clusterSize — and a majority was
// "more than half of it". That works exactly as long as the membership never
// changes, and stops working the moment it does, because the two sides of a
// membership change disagree about what a majority IS. §6 opens with the reason:
//
//	"any approach where servers switch directly from the old configuration to the
//	new configuration is unsafe. It isn't possible to atomically switch all of
//	the servers at once, so the cluster can potentially split into two
//	independent majorities during the transition."
//
// The remedy is joint consensus: a transitional configuration in which decisions
// need a majority of BOTH the old and the new membership. Neither side can act
// alone, so the two majorities cannot diverge, and servers are free to adopt the
// new configuration at different times — which they inevitably will.
//
// A Configuration is therefore either simple (Voters alone) or joint (Voters and
// OldVoters both active). Everything that used to compare a count against
// clusterSize/2 now asks the configuration instead, so joint mode is a property
// of this type rather than a special case scattered through the callers.

import (
	"fmt"
	"sort"
	"strings"
)

// Configuration describes cluster membership at a point in the log.
type Configuration struct {
	// Voters is the current membership: C_new during a joint transition, or
	// simply the membership when there is no transition in progress.
	Voters map[int]string `json:"voters"`

	// OldVoters is non-nil only during joint consensus, holding C_old. While it
	// is set, agreement requires separate majorities from both sets.
	OldVoters map[int]string `json:"old_voters,omitempty"`

	// Learners receive the log but are not counted in any majority (§6: "new
	// servers join the cluster as non-voting members... Once the new servers have
	// caught up with the rest of the cluster, the reconfiguration can proceed").
	//
	// They exist to close an availability gap: a server added cold has none of
	// the log, and counting it immediately would mean a majority that includes a
	// server unable to vote usefully for however long catch-up takes.
	Learners map[int]string `json:"learners,omitempty"`
}

// NewConfiguration builds a simple (non-joint) configuration.
func NewConfiguration(voters map[int]string) Configuration {
	return Configuration{Voters: copyMembers(voters)}
}

// IsJoint reports whether this configuration is mid-transition, requiring
// agreement from both memberships.
func (c Configuration) IsJoint() bool { return len(c.OldVoters) > 0 }

// IsVoter reports whether id counts toward a majority in this configuration.
func (c Configuration) IsVoter(id int) bool {
	if _, ok := c.Voters[id]; ok {
		return true
	}
	_, ok := c.OldVoters[id]
	return ok
}

// IsMember reports whether id takes part at all, voter or learner. The leader
// replicates to every member; only voters are counted.
func (c Configuration) IsMember(id int) bool {
	if c.IsVoter(id) {
		return true
	}
	_, ok := c.Learners[id]
	return ok
}

// Members returns every server the leader must replicate to — both
// configurations plus learners — as id → address.
func (c Configuration) Members() map[int]string {
	out := make(map[int]string, len(c.Voters)+len(c.OldVoters)+len(c.Learners))
	for _, set := range []map[int]string{c.OldVoters, c.Voters, c.Learners} {
		for id, addr := range set {
			out[id] = addr
		}
	}
	return out
}

// Peers returns every member except self.
func (c Configuration) Peers(self int) map[int]string {
	out := c.Members()
	delete(out, self)
	return out
}

// HasQuorum reports whether the given set of servers constitutes agreement.
//
// During a joint configuration this requires a majority of BOTH memberships,
// which is the entire safety content of §6: "Agreement (for elections and entry
// commitment) requires separate majorities from both the old and new
// configurations." A single combined majority would not do — the old and new
// memberships can differ enough that one majority spans neither.
func (c Configuration) HasQuorum(have map[int]bool) bool {
	if !hasMajority(c.Voters, have) {
		return false
	}
	if c.IsJoint() && !hasMajority(c.OldVoters, have) {
		return false
	}
	return true
}

func hasMajority(members map[int]string, have map[int]bool) bool {
	if len(members) == 0 {
		// An empty membership cannot withhold agreement. This arises only for the
		// absent OldVoters of a simple configuration, which IsJoint already
		// excludes; treating it as satisfied keeps HasQuorum total.
		return true
	}
	count := 0
	for id := range members {
		if have[id] {
			count++
		}
	}
	return count > len(members)/2
}

// QuorumIndex returns the highest log index replicated on a majority — of both
// memberships when joint.
//
// matchIndex must cover every voter, including the leader itself (whose entry is
// its own last log index). Indices for non-voters are ignored: a learner holding
// an entry is not evidence that the entry is committed, and counting one would
// manufacture a majority that does not exist.
func (c Configuration) QuorumIndex(matchIndex map[int]int) int {
	idx := majorityIndex(c.Voters, matchIndex)
	if !c.IsJoint() {
		return idx
	}
	// Committed means committed in BOTH memberships, so the frontier is the
	// lower of the two.
	if old := majorityIndex(c.OldVoters, matchIndex); old < idx {
		return old
	}
	return idx
}

// majorityIndex is the highest index N such that a majority of members have
// replicated at least N.
func majorityIndex(members map[int]string, matchIndex map[int]int) int {
	if len(members) == 0 {
		return 0
	}
	indices := make([]int, 0, len(members))
	for id := range members {
		indices = append(indices, matchIndex[id])
	}
	// Sort descending; the element at len/2 is the highest index held by a
	// majority. For 3 members that is the 2nd highest, for 5 the 3rd.
	sort.Sort(sort.Reverse(sort.IntSlice(indices)))
	return indices[len(indices)/2]
}

// EnterJoint returns the joint configuration transitioning from c to target.
// The current voters become C_old; target becomes C_new. Learners named in
// target are carried through; anyone promoted to voter stops being a learner.
func (c Configuration) EnterJoint(target map[int]string, learners map[int]string) Configuration {
	next := Configuration{
		Voters:    copyMembers(target),
		OldVoters: copyMembers(c.Voters),
		Learners:  copyMembers(learners),
	}
	for id := range next.Voters {
		delete(next.Learners, id)
	}
	return next
}

// LeaveJoint returns the simple configuration this joint one is heading toward:
// C_new alone. §6: once C_old,new is committed it is safe to create the entry
// describing C_new, and once THAT commits the old configuration is irrelevant.
func (c Configuration) LeaveJoint() Configuration {
	return Configuration{
		Voters:   copyMembers(c.Voters),
		Learners: copyMembers(c.Learners),
	}
}

// Clone returns a deep copy, so a configuration handed out cannot be mutated
// underneath the node that owns it.
func (c Configuration) Clone() Configuration {
	return Configuration{
		Voters:    copyMembers(c.Voters),
		OldVoters: copyMembers(c.OldVoters),
		Learners:  copyMembers(c.Learners),
	}
}

// Validate rejects configurations that could not safely be used.
func (c Configuration) Validate() error {
	if len(c.Voters) == 0 {
		return fmt.Errorf("configuration has no voters")
	}
	for id, addr := range c.Voters {
		if id <= 0 {
			return fmt.Errorf("voter id must be positive, got %d", id)
		}
		if addr == "" {
			return fmt.Errorf("voter %d has no address", id)
		}
	}
	for id := range c.Learners {
		if _, dup := c.Voters[id]; dup {
			return fmt.Errorf("server %d is both a voter and a learner", id)
		}
	}
	return nil
}

// String renders a configuration for logs.
func (c Configuration) String() string {
	var b strings.Builder
	b.WriteString("voters=" + renderMembers(c.Voters))
	if c.IsJoint() {
		b.WriteString(" old=" + renderMembers(c.OldVoters))
	}
	if len(c.Learners) > 0 {
		b.WriteString(" learners=" + renderMembers(c.Learners))
	}
	return b.String()
}

func renderMembers(m map[int]string) string {
	ids := make([]int, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%d", id))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func copyMembers(m map[int]string) map[int]string {
	if m == nil {
		return nil
	}
	out := make(map[int]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
