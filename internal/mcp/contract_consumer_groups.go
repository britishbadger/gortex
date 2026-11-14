package mcp

import (
	"context"
	"slices"
	"sync"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/zzet/gortex/internal/graphview"
	"github.com/zzet/gortex/internal/indexer"
)

// contractConsumerGroups splits an admitted cohort by effective workspace.
// The matcher pairs, and types resolve, only within one workspace, so each
// is analysed as its own cohort: a move in one never supersedes another's
// job. A cohort of one workspace keeps its single actor and keys, as does a
// routed view's request, whose companions are captured through the selected
// view under its checkout. Targets keep only the groups that hold them.
func (s *Server) contractConsumerGroups(repos, targets []string, routed bool) [][]string {
	groups := [][]string{repos}
	if s.multiIndexer != nil && len(repos) > 1 && !routed {
		if split := s.multiIndexer.RepoWorkspaceGroups(repos); len(split) > 1 {
			groups = split
		}
	}
	if len(targets) == 0 || len(groups) == 1 {
		return groups
	}
	var kept [][]string
	for _, group := range groups {
		if slices.ContainsFunc(targets, func(repo string) bool { return slices.Contains(group, repo) }) {
			kept = append(kept, group)
		}
	}
	if len(kept) == 0 {
		return groups
	}
	return kept
}

func contractGroupActors(checkout string, groups [][]string) []indexer.ContractActor {
	actors := make([]indexer.ContractActor, 0, len(groups))
	for _, group := range groups {
		actors = append(actors, indexer.NewContractActor(checkout, group))
	}
	return actors
}

func contractGroupTargets(targets, group []string) []string {
	var kept []string
	for _, repo := range targets {
		if slices.Contains(group, repo) {
			kept = append(kept, repo)
		}
	}
	return kept
}

// awaitContractGroups waits for every workspace group at once, each as
// awaitContractCohort waits for a whole cohort. The merged answer is fresh
// only when every group is; served when every group has a current view or a
// served predecessor; otherwise the first group's refusal.
func (s *Server) awaitContractGroups(ctx, waitCtx context.Context, status *contractConsumerStatus, view *requestView, selected *graphview.RepoView, checkout string, groups [][]string, targets []string) (context.Context, *mcp.CallToolResult) {
	siblings := contractGroupActors(checkout, groups)
	parts := make([]*contractAnalysisContext, len(groups))
	refusals := make([]*mcp.CallToolResult, len(groups))
	statuses := make([]*contractConsumerStatus, len(groups))
	panics := make([]any, len(groups))
	share := &contractWaitShare{s: s, groups: len(groups)}
	waitCtx = context.WithValue(waitCtx, contractWaitShareKey{}, share)
	var wg sync.WaitGroup
	for i, group := range groups {
		statuses[i] = &contractConsumerStatus{mode: status.mode, state: graphview.StateIncomplete, reason: "no_attachment", allowServed: status.allowServed}
		wg.Add(1)
		go func() {
			defer wg.Done()
			// A finished group no longer works for the request.
			defer share.enter()
			// A panic is raised again on the request's own goroutine.
			defer func() { panics[i] = recover() }()
			parts[i], refusals[i] = s.awaitContractCohort(ctx, waitCtx, statuses[i], nil, selected, checkout, group, contractGroupTargets(targets, group), siblings)
		}()
	}
	wg.Wait()
	share.leave()
	closeParts := func() {
		for _, part := range parts {
			part.close()
		}
	}
	for _, recovered := range panics {
		if recovered != nil {
			closeParts()
			panic(recovered)
		}
	}
	for _, refusal := range refusals {
		if refusal != nil {
			closeParts()
			return ctx, refusal
		}
	}
	binding := mergeContractGroupBindings(parts, &s.contractRegistries)
	binding.served = status.allowServed
	fresh := true
	for _, part := range statuses {
		fresh = fresh && part.fresh
		status.receiptsUnknown = status.receiptsUnknown || part.receiptsUnknown
	}
	if fresh {
		status.state, status.reason, status.fresh = graphview.StateComplete, "", true
		if view != nil {
			view.contractCompleteness = &status.state
		}
	} else {
		status.state, status.reason, status.fresh = graphview.StateBuilding, "contract_analysis_pending", false
		if view != nil {
			// Every served group payload is complete; the request's current
			// keys are still building.
			complete := graphview.StateComplete
			view.contractCompleteness = &complete
		}
	}
	return withContractAnalysisContext(ctx, binding), nil
}

// mergeContractGroupBindings moves each group's views and leases into one
// binding that remembers each view's group: input agreement holds within a
// group, never across groups.
func mergeContractGroupBindings(parts []*contractAnalysisContext, registries *contractRegistryCache) *contractAnalysisContext {
	merged := &contractAnalysisContext{views: make(map[string]*graphview.ContractAnalysisView), cohortOf: make(map[string]int), registries: registries}
	var releases []func()
	for group, part := range parts {
		if part == nil {
			continue
		}
		for repo, view := range part.views {
			merged.views[repo] = view
			merged.cohortOf[repo] = group
		}
		if part.releaseCohort != nil {
			releases = append(releases, part.releaseCohort)
		}
		merged.recovered = merged.recovered || part.recovered
		part.views, part.releaseCohort = nil, nil
	}
	if len(releases) > 0 {
		merged.releaseCohort = func() {
			for _, release := range releases {
				release()
			}
		}
	}
	return merged
}

type contractWaitShareKey struct{}

// contractWaitShare counts the groups of one split request that wait or are
// done. The request is one suspended producer waiter while all of them are,
// and foreground demand while any group still captures or opens.
type contractWaitShare struct {
	s      *Server
	groups int
	mu     sync.Mutex
	idle   int
}

func (w *contractWaitShare) enter() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.idle++
	if w.idle == w.groups {
		w.s.contractAnalysisWaiters.Add(1)
	}
}

func (w *contractWaitShare) leave() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.idle == w.groups {
		w.s.contractAnalysisWaiters.Add(-1)
	}
	w.idle--
}
