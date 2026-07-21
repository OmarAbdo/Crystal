package raft

import "testing"

func members(ids ...int) map[int]string {
	m := make(map[int]string, len(ids))
	for _, id := range ids {
		m[id] = "addr"
	}
	return m
}

func votes(ids ...int) map[int]bool {
	v := make(map[int]bool, len(ids))
	for _, id := range ids {
		v[id] = true
	}
	return v
}

func TestHasQuorum_SimpleConfiguration(t *testing.T) {
	c := NewConfiguration(members(1, 2, 3))

	if c.HasQuorum(votes(1)) {
		t.Fatal("1 of 3 is not a majority")
	}
	if !c.HasQuorum(votes(1, 2)) {
		t.Fatal("2 of 3 is a majority")
	}
	if !c.HasQuorum(votes(1, 2, 3)) {
		t.Fatal("3 of 3 is a majority")
	}
}

func TestHasQuorum_EvenSizedConfiguration(t *testing.T) {
	c := NewConfiguration(members(1, 2, 3, 4))

	if c.HasQuorum(votes(1, 2)) {
		t.Fatal("2 of 4 is a tie, not a majority")
	}
	if !c.HasQuorum(votes(1, 2, 3)) {
		t.Fatal("3 of 4 is a majority")
	}
}

// The heart of §6: during a transition, agreement requires separate majorities
// from BOTH memberships. A single combined majority is not enough, because the
// two memberships can differ enough that one majority spans neither — which is
// exactly the split the joint phase exists to prevent.
func TestHasQuorum_JointRequiresBothMajorities(t *testing.T) {
	// Growing {1,2,3} to {3,4,5}: the memberships overlap only at node 3.
	c := NewConfiguration(members(1, 2, 3)).EnterJoint(members(3, 4, 5), nil)

	if !c.IsJoint() {
		t.Fatal("expected a joint configuration")
	}

	// A majority of C_old alone.
	if c.HasQuorum(votes(1, 2)) {
		t.Fatal("a majority of the OLD configuration alone must not agree")
	}
	// A majority of C_new alone.
	if c.HasQuorum(votes(4, 5)) {
		t.Fatal("a majority of the NEW configuration alone must not agree")
	}
	// Four of six servers, but only one from C_old — not a majority there.
	if c.HasQuorum(votes(3, 4, 5)) {
		t.Fatal("{3,4,5} is a majority of C_new but only 1 of 3 in C_old")
	}
	// Majorities in both.
	if !c.HasQuorum(votes(1, 2, 4, 5)) {
		t.Fatal("{1,2} in C_old and {4,5} in C_new is agreement in both")
	}
	// The overlap counts for both memberships at once.
	if !c.HasQuorum(votes(1, 3, 4)) {
		t.Fatal("{1,3} in C_old and {3,4} in C_new is agreement in both")
	}
}

// Committed means committed in both memberships, so the commit frontier during a
// transition is the LOWER of the two — never the more generous one.
func TestQuorumIndex_JointTakesTheLowerFrontier(t *testing.T) {
	c := NewConfiguration(members(1, 2, 3)).EnterJoint(members(4, 5, 6), nil)

	match := map[int]int{
		1: 10, 2: 10, 3: 10, // C_old is well ahead
		4: 5, 5: 5, 6: 0, // C_new lags
	}
	if got := c.QuorumIndex(match); got != 5 {
		t.Fatalf("QuorumIndex = %d, want 5 — the frontier must be the lower of the "+
			"two memberships, not the old one's", got)
	}

	// And symmetrically when the new configuration is ahead.
	match = map[int]int{1: 3, 2: 3, 3: 0, 4: 9, 5: 9, 6: 9}
	if got := c.QuorumIndex(match); got != 3 {
		t.Fatalf("QuorumIndex = %d, want 3", got)
	}
}

func TestQuorumIndex_SimpleConfiguration(t *testing.T) {
	c := NewConfiguration(members(1, 2, 3))

	// Sorted descending {7,5,2}; the majority frontier is the middle one.
	if got := c.QuorumIndex(map[int]int{1: 7, 2: 5, 3: 2}); got != 5 {
		t.Fatalf("QuorumIndex = %d, want 5", got)
	}
	// A single node is its own majority.
	single := NewConfiguration(members(1))
	if got := single.QuorumIndex(map[int]int{1: 42}); got != 42 {
		t.Fatalf("single-node QuorumIndex = %d, want 42", got)
	}
}

// A learner holding an entry is not evidence that the entry is committed.
// Counting one would manufacture a majority that does not exist.
func TestQuorumIndex_IgnoresLearners(t *testing.T) {
	c := Configuration{
		Voters:   members(1, 2, 3),
		Learners: members(9),
	}

	match := map[int]int{1: 10, 2: 2, 3: 2, 9: 10}
	if got := c.QuorumIndex(match); got != 2 {
		t.Fatalf("QuorumIndex = %d, want 2 — a learner was counted toward the "+
			"majority", got)
	}
}

func TestLearners_ParticipateButDoNotVote(t *testing.T) {
	c := Configuration{Voters: members(1, 2, 3), Learners: members(9)}

	if c.IsVoter(9) {
		t.Fatal("a learner must not be a voter")
	}
	if !c.IsMember(9) {
		t.Fatal("a learner is still a member: the leader replicates to it")
	}
	if _, ok := c.Members()[9]; !ok {
		t.Fatal("Members must include learners — they receive the log")
	}
	// A learner's vote cannot make a quorum.
	if c.HasQuorum(votes(1, 9)) {
		t.Fatal("a learner's agreement was counted toward a majority")
	}
}

func TestEnterAndLeaveJoint(t *testing.T) {
	start := NewConfiguration(members(1, 2, 3))

	joint := start.EnterJoint(members(1, 2, 4), nil)
	if !joint.IsJoint() {
		t.Fatal("EnterJoint did not produce a joint configuration")
	}
	if !joint.IsVoter(3) {
		t.Fatal("a server being removed must still vote during the joint phase")
	}
	if !joint.IsVoter(4) {
		t.Fatal("a server being added must vote during the joint phase")
	}

	final := joint.LeaveJoint()
	if final.IsJoint() {
		t.Fatal("LeaveJoint left the configuration joint")
	}
	if final.IsVoter(3) {
		t.Fatal("the removed server is still a voter after the transition")
	}
	if !final.IsVoter(4) {
		t.Fatal("the added server is not a voter after the transition")
	}
}

// Promotion: a learner named as a voter in the target stops being a learner,
// or it would be counted in neither set and replicated to twice.
func TestEnterJoint_PromotesLearnerToVoter(t *testing.T) {
	start := Configuration{Voters: members(1, 2, 3), Learners: members(4)}

	joint := start.EnterJoint(members(1, 2, 3, 4), start.Learners)
	if !joint.IsVoter(4) {
		t.Fatal("the promoted learner is not a voter")
	}
	if _, still := joint.Learners[4]; still {
		t.Fatal("the promoted server is still listed as a learner")
	}
}

func TestPeers_ExcludesSelf(t *testing.T) {
	c := Configuration{Voters: members(1, 2, 3), Learners: members(9)}

	peers := c.Peers(1)
	if _, self := peers[1]; self {
		t.Fatal("Peers included self")
	}
	if len(peers) != 3 {
		t.Fatalf("Peers = %v, want nodes 2, 3 and learner 9", peers)
	}
}

func TestValidate(t *testing.T) {
	if err := NewConfiguration(nil).Validate(); err == nil {
		t.Fatal("a configuration with no voters must be rejected")
	}
	if err := NewConfiguration(map[int]string{1: ""}).Validate(); err == nil {
		t.Fatal("a voter without an address must be rejected")
	}
	bad := Configuration{Voters: members(1, 2), Learners: members(2)}
	if err := bad.Validate(); err == nil {
		t.Fatal("a server that is both voter and learner must be rejected")
	}
	if err := NewConfiguration(members(1, 2, 3)).Validate(); err != nil {
		t.Fatalf("valid configuration rejected: %v", err)
	}
}

// Clone must be deep, or a caller mutating what it was handed would silently
// change the configuration the node is making quorum decisions against.
func TestClone_IsDeep(t *testing.T) {
	c := NewConfiguration(members(1, 2, 3))
	clone := c.Clone()
	delete(clone.Voters, 1)

	if !c.IsVoter(1) {
		t.Fatal("mutating a clone changed the original configuration")
	}
}
