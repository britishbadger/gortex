package indexer

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zzet/gortex/internal/graph"
	"github.com/zzet/gortex/internal/viewmetrics"
)

// One request split into workspace groups is one admission unit: all of
// its groups are admitted past QueueCapacity, and past the registry bound,
// while the split takes one queue place, so other new actors still are.
func TestContractSchedulerAdmitsEverySiblingAsOneUnit(t *testing.T) {
	p := newSchedulerProbe(t, func(options *ContractAnalysisCoordinatorOptions) { options.QueueCapacity = 8 })
	schedule := func(actor ContractActor, siblings []ContractActor) error {
		t.Helper()
		repo := actor.Repos()[0]
		p.hold(repo + "/@k")
		_, err := p.c.Schedule(t.Context(), ContractScheduleRequest{Actor: actor, Key: graph.ContractAttachmentKey{RepoPrefix: repo}, Inputs: schedulerInputs([]string{repo}, "k"), Epoch: p.c.InputEpoch(), Siblings: siblings})
		return err
	}
	var actors []ContractActor
	for i := range contractSchedulerMaxActors + 6 {
		actors = append(actors, NewContractActor("", []string{fmt.Sprintf("g%02d", i)}))
	}
	for i, actor := range actors {
		require.NoError(t, schedule(actor, actors), "group %d of %d", i, len(actors))
	}
	for i := range 7 {
		require.NoError(t, schedule(NewContractActor("", []string{fmt.Sprintf("lone%d", i)}), nil), "the split takes one queue place")
	}
	require.ErrorIs(t, schedule(NewContractActor("", []string{"lone7"}), nil), ErrContractQueueFull, "an actor of no admitted split still meets the capacity")
	pair := []ContractActor{NewContractActor("", []string{"x"}), NewContractActor("", []string{"y"})}
	require.ErrorIs(t, schedule(pair[0], pair), ErrContractQueueFull, "the first actor of a split meets the capacity")
	p.c.mu.Lock()
	load := p.c.admissionLocked(p.c.now(), contractSplitKey(actors))
	p.c.mu.Unlock()
	require.Equal(t, 8, load.active)
	require.True(t, load.joinsActive)
}

// The actors of one split are evicted only together, and only once none has
// work or interest: a group that stayed current, and so was not asked for
// again, keeps its last published cohort while a sibling is still wanted.
func TestContractSchedulerRegistryEvictsSplitsWhole(t *testing.T) {
	ring := &contractRing{repo: "r", wake: make(chan struct{}, 1)}
	c := &ContractAnalysisCoordinator{actors: make(map[ContractActor]*contractActor), rings: map[string]*contractRing{"r": ring}}
	now := time.Now()
	var groups []ContractActor
	for i := range 3 {
		groups = append(groups, NewContractActor(fmt.Sprintf("group-%d", i), []string{"r"}))
	}
	split := contractSplitKey(groups)
	for i, id := range groups {
		a := c.registerLocked(id, split, now.Add(-time.Hour))
		a.lastCohort = &ContractPublishedCohort{}
		if i == 0 {
			a.interestUntil = now.Add(time.Minute)
		}
	}
	for i := range contractSchedulerMaxActors - 1 {
		c.registerLocked(NewContractActor(fmt.Sprintf("checkout-%d", i), []string{"r"}), "", now.Add(time.Duration(i)*time.Millisecond))
	}
	c.registerLocked(NewContractActor("new", []string{"r"}), "", now)
	for _, id := range groups {
		require.Contains(t, c.actors, id, "a split a request still wants keeps every group")
		require.NotNil(t, c.actors[id].lastCohort)
	}
	require.NotContains(t, c.actors, NewContractActor("checkout-0", []string{"r"}), "the least recent idle unit is evicted instead")
	groups = append(groups, NewContractActor("joiner", []string{"r"}))
	c.registerLocked(groups[3], split, now)
	require.Len(t, c.actors, contractSchedulerMaxActors+3, "a sibling joins its split's place")

	c.actors[groups[0]].interestUntil = now.Add(-time.Second)
	for _, id := range groups {
		c.actors[id].lastUsed = now.Add(-2 * time.Hour)
	}
	c.registerLocked(NewContractActor("later", []string{"r"}), "", now)
	for _, id := range groups {
		require.NotContains(t, c.actors, id, "an unwanted split is evicted whole")
	}
	require.Len(t, c.actors, contractSchedulerMaxActors)
}

// An idle actor a request still has interest in keeps its custody, and its
// last published cohort, however full the registry is.
func TestContractSchedulerRegistryNeverEvictsAnInterestedActor(t *testing.T) {
	ring := &contractRing{repo: "r", wake: make(chan struct{}, 1)}
	c := &ContractAnalysisCoordinator{actors: make(map[ContractActor]*contractActor), rings: map[string]*contractRing{"r": ring}}
	now := time.Now()
	for i := range contractSchedulerMaxActors {
		id := NewContractActor(fmt.Sprintf("checkout-%d", i), []string{"r"})
		a := c.registerLocked(id, "", now.Add(time.Duration(i)*time.Millisecond))
		a.interestUntil = now.Add(time.Minute)
		a.lastCohort = &ContractPublishedCohort{}
	}
	newID := NewContractActor("new", []string{"r"})
	c.registerLocked(newID, "", now).interestUntil = now.Add(time.Minute)
	require.Len(t, c.actors, contractSchedulerMaxActors+1, "no interested actor is evicted")
	for id, a := range c.actors {
		if id != newID {
			require.NotNil(t, a.lastCohort)
		}
	}
	expired := NewContractActor("checkout-3", []string{"r"})
	c.actors[expired].interestUntil = now.Add(-time.Second)
	c.registerLocked(NewContractActor("later", []string{"r"}), "", now)
	require.NotContains(t, c.actors, expired, "an actor whose interest ran out is evicted")
	require.Len(t, c.actors, contractSchedulerMaxActors+1)
}

// Workspace groups are separate actors: newer inputs of one supersede only
// that group's job.
func TestContractSchedulerGroupMoveSupersedesOnlyItsOwnJob(t *testing.T) {
	p := newSchedulerProbe(t, func(options *ContractAnalysisCoordinatorOptions) { options.JobSlots = 2 })
	jobs := make(chan *contractAnalysisJob, 2)
	resume := make(chan struct{})
	defer close(resume)
	p.c.run = func(job *contractAnalysisJob) string {
		defer job.snapshot.Release()
		select {
		case jobs <- job:
		case <-p.c.ctx.Done():
			return viewmetrics.ContractJobPreempted
		}
		select {
		case <-resume:
		case <-p.c.ctx.Done():
		}
		return viewmetrics.ContractJobPreempted
	}
	a, b := NewContractActor("", []string{"a"}), NewContractActor("", []string{"b"})
	siblings := []ContractActor{a, b}
	schedule := func(actor ContractActor, key string) {
		t.Helper()
		repo := actor.Repos()[0]
		_, err := p.c.Schedule(t.Context(), ContractScheduleRequest{Actor: actor, Key: graph.ContractAttachmentKey{RepoPrefix: repo}, Inputs: schedulerInputs([]string{repo}, key), Epoch: p.c.InputEpoch(), Siblings: siblings})
		require.NoError(t, err)
	}
	schedule(a, "a1")
	schedule(b, "b1")
	running := make(map[string]*contractAnalysisJob)
	for range 2 {
		select {
		case job := <-jobs:
			running[job.snapshot.Key.RepoPrefix] = job
		case <-time.After(time.Minute):
			t.Fatal("scheduler never dispatched both groups")
		}
	}
	require.NoError(t, running["a"].backgroundCurrent(t.Context()))
	require.NoError(t, running["b"].backgroundCurrent(t.Context()))
	schedule(b, "b2")
	require.ErrorIs(t, running["b"].backgroundCurrent(t.Context()), errContractJobKeyMoved, "b's move supersedes b's job")
	require.NoError(t, running["a"].backgroundCurrent(t.Context()), "a's job is untouched")
}

// splitProbe is a scheduler with one queue place and a clock tests advance.
func splitProbe(t *testing.T) (*schedulerProbe, *atomic.Int64, func(ContractActor, string, []ContractActor) (ContractScheduleResult, error)) {
	t.Helper()
	var offset atomic.Int64
	p := newSchedulerProbe(t, func(options *ContractAnalysisCoordinatorOptions) {
		options.QueueCapacity = 1
		options.Now = func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	})
	schedule := func(actor ContractActor, key string, siblings []ContractActor) (ContractScheduleResult, error) {
		t.Helper()
		repo := actor.Repos()[0]
		return p.c.Schedule(t.Context(), ContractScheduleRequest{Actor: actor, Key: graph.ContractAttachmentKey{RepoPrefix: repo}, Inputs: schedulerInputs([]string{repo}, key), Epoch: p.c.InputEpoch(), Siblings: siblings})
	}
	return p, &offset, schedule
}

func (p *schedulerProbe) unitOf(actor ContractActor) contractAdmissionUnit {
	p.c.mu.Lock()
	defer p.c.mu.Unlock()
	return p.c.actors[actor].unit()
}

// A targeted request that shares an actor with an unscoped one, alone or
// over two of its groups, leaves the actor in the unscoped split while that
// split is wanted: the split's other groups still join its one place, and
// the split is still evicted only whole.
func TestContractSchedulerOverlappingRequestsKeepSplitMembership(t *testing.T) {
	p, offset, schedule := splitProbe(t)
	a, b, c := NewContractActor("", []string{"a"}), NewContractActor("", []string{"b"}), NewContractActor("", []string{"c"})
	for _, repo := range []string{"a", "b", "c"} {
		p.hold(repo + "/@k")
	}
	unscoped := []ContractActor{a, b, c}
	whole := contractAdmissionUnit{split: contractSplitKey(unscoped)}
	_, err := schedule(a, "k", unscoped)
	require.NoError(t, err)
	_, err = schedule(a, "k", nil)
	require.NoError(t, err, "a request for a alone coalesces onto it")
	_, err = schedule(a, "k", []ContractActor{a, c})
	require.NoError(t, err, "a request over a and c coalesces onto a")
	_, err = schedule(b, "k", unscoped)
	require.NoError(t, err, "b joins its split's place")
	_, err = schedule(c, "k", unscoped)
	require.NoError(t, err, "c joins its split's place")
	_, err = schedule(NewContractActor("", []string{"d"}), "k", nil)
	require.ErrorIs(t, err, ErrContractQueueFull, "the split still takes its one place")
	for _, actor := range unscoped {
		require.Equal(t, whole, p.unitOf(actor), "every group stays in the split, so it is evicted whole")
	}

	for _, repo := range []string{"a", "b", "c"} {
		p.release(repo + "/@k")
	}
	require.Eventually(t, func() bool {
		for _, actor := range unscoped {
			if p.c.Status(actor).State != contractActorIdle.String() {
				return false
			}
		}
		return true
	}, 10*time.Second, time.Millisecond)
	offset.Store(int64(contractInterestTTL + time.Second))
	_, err = schedule(a, "k2", nil)
	require.NoError(t, err)
	require.Equal(t, contractAdmissionUnit{actor: a}, p.unitOf(a), "once its split is no longer wanted, a request for a alone takes it out")
}

// A split keeps its place while a request still wants it, not only while a
// group is queued or running: a group that finished before its sibling was
// asked for does not hand the split's place to other work.
func TestContractSchedulerSplitHoldsItsPlaceBetweenSiblings(t *testing.T) {
	p, offset, schedule := splitProbe(t)
	a, b, other := NewContractActor("", []string{"a"}), NewContractActor("", []string{"b"}), NewContractActor("", []string{"c"})
	split := []ContractActor{a, b}
	first, err := schedule(a, "k", split)
	require.NoError(t, err)
	require.Equal(t, ContractActorPublished, p.wait(first).Result)
	require.Eventually(t, func() bool { return p.c.Status(a).State == contractActorIdle.String() }, 10*time.Second, time.Millisecond)

	_, err = schedule(other, "k", nil)
	require.ErrorIs(t, err, ErrContractQueueFull, "other work does not take the place of a split still wanted")
	second, err := schedule(b, "k", split)
	require.NoError(t, err, "b joins its split's place after a finished")
	require.Equal(t, ContractActorPublished, p.wait(second).Result)
	require.Eventually(t, func() bool { return p.c.Status(b).State == contractActorIdle.String() }, 10*time.Second, time.Millisecond)

	_, err = schedule(other, "k", nil)
	require.ErrorIs(t, err, ErrContractQueueFull, "the place holds while the request's interest lasts")
	offset.Store(int64(contractInterestTTL + time.Second))
	_, err = schedule(other, "k", nil)
	require.NoError(t, err, "the place frees once the split's interest ends")
}
