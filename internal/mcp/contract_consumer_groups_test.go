package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zzet/gortex/internal/config"
	"github.com/zzet/gortex/internal/graph"
	"github.com/zzet/gortex/internal/graph/store_sqlite"
	"github.com/zzet/gortex/internal/graphview"
	"github.com/zzet/gortex/internal/indexer"
	"github.com/zzet/gortex/internal/search"
)

// newWorkspaceGroupFixture is the consumer fixture with repo and b tracked in
// the named workspaces.
func newWorkspaceGroupFixture(t *testing.T, repoWorkspace, bWorkspace string) *contractConsumerFixture {
	t.Helper()
	f := newContractConsumerFixture(t)
	var entries []config.RepoEntry
	for _, repo := range []string{"repo", "b"} {
		workspace := repoWorkspace
		if repo == "b" {
			workspace = bWorkspace
		}
		root := filepath.Join(f.root, repo)
		require.NoError(t, os.WriteFile(filepath.Join(root, ".gortex.yaml"), []byte("workspace: "+workspace+"\n"), 0o644))
		entries = append(entries, config.RepoEntry{Path: root, Name: repo})
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	global := &config.GlobalConfig{Repos: entries}
	global.SetConfigPath(path)
	require.NoError(t, global.Save())
	manager, err := config.NewConfigManager(path)
	require.NoError(t, err)
	multi := indexer.NewMultiIndexer(graph.New(), testRegistry(), search.NewNull(), manager, zap.NewNop())
	registerMCPFixtureRepos(t, multi, manager)
	f.srv.multiIndexer = multi
	return f
}

// publishWorkspaceGroups publishes each group as its own cohort: one route
// per repo, /<repo><suffix>, or route when it is set.
func publishWorkspaceGroups(t *testing.T, f *contractConsumerFixture, groups [][]string, suffix, route string) {
	t.Helper()
	for _, group := range groups {
		var captures []*graphview.SelectedContractInputs
		for _, repo := range group {
			input, err := f.srv.materializer.CaptureContractInputs(t.Context(), nil, repo, "")
			require.NoError(t, err)
			captures = append(captures, input)
		}
		for _, repo := range group {
			input, err := graphview.ComposeSelectedContractInputs(repo, "", captures...)
			require.NoError(t, err)
			path := "/" + repo + suffix
			if route != "" {
				path = route
			}
			id := "http::GET::" + path
			meta := map[string]any{"path": path, "method": "GET"}
			canonical := &graph.Node{ID: id, Name: "GET " + path, Kind: graph.KindContract, FilePath: repo + "/handler.go", RepoPrefix: repo, Meta: map[string]any{"type": "http", "role": "provider", "symbol_id": repo + "/handler.go::Handle", "line": 2, "contract_owner_record": true, "contract_meta": meta}}
			owner := &graph.Edge{From: repo + "/handler.go::Handle", To: id, Kind: graph.EdgeProvides, FilePath: repo + "/handler.go", Meta: map[string]any{"contract_owner_repo_prefix": repo, "contract_owner_type": "http", "contract_owner_symbol_id": repo + "/handler.go::Handle", "contract_owner_meta": meta}}
			f.sequence++
			generation, handle, err := f.store.BeginPayloadGeneration(t.Context(), store_sqlite.PayloadGenerationRequest{OwnerKind: "ref_view", GraphID: "contract-test", LayerID: fmt.Sprintf("analysis-%d", f.sequence), GenerationKind: "commit", TreeOID: fmt.Sprintf("analysis-tree-%d", f.sequence), CreatedAt: 1})
			require.NoError(t, err)
			handle.AddBatch([]*graph.Node{canonical}, []*graph.Edge{owner})
			require.NoError(t, handle.SetProducerState(store_sqlite.ProducerCompleteness{Producer: string(graphview.CapContracts), State: store_sqlite.ProducerStateComplete}))
			attachment := graph.ContractAttachment{RepoPrefix: repo, PayloadGeneration: generation, InputVersion: input.State.InputVersion, InputFingerprint: input.State.InputFingerprint}
			require.NoError(t, f.store.PublishContractAttachmentWithInputsContext(t.Context(), input.State, input.Witnesses, attachment, nil, 2))
		}
	}
}

// groupSchedules records every schedule request; waits last until the
// request's deadline.
type groupSchedules struct {
	mu       sync.Mutex
	requests []indexer.ContractScheduleRequest
}

func (g *groupSchedules) install(t *testing.T, f *contractConsumerFixture, acquire func(context.Context, indexer.ContractActor) (*indexer.ContractPublishedCohort, func(), error)) {
	fake := &contractRuntimeFake{t: t, acquire: acquire}
	fake.schedule = func(ctx context.Context, req indexer.ContractScheduleRequest) (indexer.ContractScheduleResult, error) {
		g.mu.Lock()
		g.requests = append(g.requests, req)
		g.mu.Unlock()
		return scheduledAt(1)(ctx, req)
	}
	fake.waitActor = func(ctx context.Context, _ indexer.ContractActor, _ uint64) (indexer.ContractActorStatus, error) {
		<-ctx.Done()
		return indexer.ContractActorStatus{}, ctx.Err()
	}
	f.srv.SetContractAnalysisRuntime(fake.runtime())
}

func (g *groupSchedules) actors() map[string][]string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[string][]string)
	for _, req := range g.requests {
		var witnessed []string
		for _, witness := range req.Inputs.Witnesses {
			witnessed = append(witnessed, witness.State.RepoPrefix)
		}
		sort.Strings(witnessed)
		out[strings.Join(req.Actor.Repos(), ",")] = witnessed
	}
	return out
}

func emptyCoordinatorAcquire(t *testing.T, f *contractConsumerFixture) func(context.Context, indexer.ContractActor) (*indexer.ContractPublishedCohort, func(), error) {
	c, err := indexer.NewContractAnalysisCoordinator(indexer.ContractAnalysisCoordinatorOptions{Store: f.store, Leases: f.srv.materializer.Leases, Registry: testRegistry(), Config: config.Default().Index, Yield: func(ctx context.Context) error { return ctx.Err() }, Capture: func(context.Context, *indexer.ContractCaptureSource, *graphview.SelectedContractInputs, string, string) (indexer.ContractFollowupSnapshot, []indexer.ContractFollowupTarget, error) {
		t.Error("a served predecessor started fresh analysis")
		return indexer.ContractFollowupSnapshot{}, nil, context.Canceled
	}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Close()) })
	return c.AcquireLastPublishedCohort
}

func contractAnalysisFields(t *testing.T, body string) map[string]any {
	t.Helper()
	var object map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &object))
	fields, _ := object["contract_analysis"].(map[string]any)
	require.NotNil(t, fields, body)
	return fields
}

// T1: a contract-relevant move in one workspace leaves the other
// workspace's analysis current; an unscoped list answers from it and from
// the moved workspace's served predecessor instead of refusing.
func TestContractUnscopedListServesEveryWorkspaceGroupWhenOneMoved(t *testing.T) {
	f := newWorkspaceGroupFixture(t, "wa", "wb")
	publishWorkspaceGroups(t, f, [][]string{{"b"}, {"repo"}}, "-v1", "")
	moveCompanionInput(t, f, "b-moved")
	var schedules groupSchedules
	schedules.install(t, f, emptyCoordinatorAcquire(t, f))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := f.request(ctx, "contracts", map[string]any{"action": "list", "format": "json"}, f.srv.handleContracts)
	body := requireContractBody(t, result, err)
	require.Contains(t, body, "/repo-v1", "the unmoved workspace is current")
	require.Contains(t, body, "/b-v1", "the moved workspace serves its predecessor")
	fields := contractAnalysisFields(t, body)
	require.Equal(t, "building", fields["state"])
	require.Equal(t, false, fields["fresh"])
	require.Equal(t, map[string][]string{"b": {"b"}}, schedules.actors(), "only the moved group is asked for")
}

// T1, optional consumers: the same split binds lazily without waiting.
func TestContractOptionalConsumerServesEveryWorkspaceGroupWhenOneMoved(t *testing.T) {
	f := newWorkspaceGroupFixture(t, "wa", "wb")
	publishWorkspaceGroups(t, f, [][]string{{"b"}, {"repo"}}, "-v1", "")
	moveCompanionInput(t, f, "b-moved")
	var schedules groupSchedules
	schedules.install(t, f, emptyCoordinatorAcquire(t, f))
	ctx := prepareOptionalForTest(t, t.Context(), f, false)
	registry := f.srv.optionalContractRegistryForContext(ctx)
	require.NotNil(t, registry)
	require.NotEmpty(t, registry.ByID("http::GET::/repo-v1"))
	require.NotEmpty(t, registry.ByID("http::GET::/b-v1"))
	binding := contractAnalysisFromContext(ctx)
	defer binding.close()
	require.True(t, binding.served)
	require.NoError(t, binding.validate(ctx))
	status := contractConsumerStatusFromContext(ctx)
	require.Equal(t, graphview.StateBuilding, status.state)
	require.False(t, status.fresh)
	require.Equal(t, map[string][]string{"b": {"b"}}, schedules.actors())
	ctx = prepareOptionalForTest(t, t.Context(), f, true)
	require.Nil(t, f.srv.optionalContractRegistryForContext(ctx), "an exact optional read refuses a served group")
}

// T2: an unscoped request over two workspaces schedules one actor per
// workspace over that workspace's inputs only, as one admission.
func TestContractUnscopedRequestSchedulesOneActorPerWorkspace(t *testing.T) {
	f := newWorkspaceGroupFixture(t, "wa", "wb")
	var schedules groupSchedules
	schedules.install(t, f, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := f.request(ctx, "contracts", map[string]any{"action": "list", "format": "json"}, f.srv.handleContracts)
	require.NoError(t, err)
	require.True(t, result.IsError, "nothing was published")
	require.Equal(t, map[string][]string{"b": {"b"}, "repo": {"repo"}}, schedules.actors())
	siblings := []indexer.ContractActor{indexer.NewContractActor("", []string{"b"}), indexer.NewContractActor("", []string{"repo"})}
	for _, req := range schedules.requests {
		require.ElementsMatch(t, siblings, req.Siblings)
	}
}

// T3: repos of one workspace keep today's single actor and key.
func TestContractSingleWorkspaceRequestKeepsTheSingleActorKey(t *testing.T) {
	f := newWorkspaceGroupFixture(t, "ws", "ws")
	var schedules groupSchedules
	schedules.install(t, f, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := f.request(ctx, "contracts", map[string]any{"action": "list", "format": "json"}, f.srv.handleContracts)
	require.NoError(t, err)
	require.True(t, result.IsError)
	var captures []*graphview.SelectedContractInputs
	for _, repo := range []string{"b", "repo"} {
		input, err := f.srv.materializer.CaptureContractInputs(t.Context(), nil, repo, "")
		require.NoError(t, err)
		captures = append(captures, input)
	}
	want, err := graphview.ComposeSelectedContractInputs("b", "", captures...)
	require.NoError(t, err)
	require.NotEmpty(t, schedules.requests)
	for _, req := range schedules.requests {
		require.Equal(t, indexer.NewContractActor("", []string{"b", "repo"}), req.Actor)
		require.Equal(t, graph.ContractAttachmentKey{RepoPrefix: "b", InputVersion: want.State.InputVersion, InputFingerprint: want.State.InputFingerprint}, req.Key)
		require.Equal(t, want.Witnesses, req.Inputs.Witnesses)
		require.Empty(t, req.Siblings)
	}
}

// T4: a contract ID both workspaces define is one row, chosen the way a
// single cohort chooses it, whichever group is read first; each group keeps
// its own input agreement.
func TestContractWorkspaceGroupsMergeSharedContractRows(t *testing.T) {
	f := newWorkspaceGroupFixture(t, "wa", "wb")
	publishWorkspaceGroups(t, f, [][]string{{"b"}, {"repo"}}, "", "/shared")
	installForbiddenContractRuntime(t, f.srv) // Both groups are current.
	result, err := f.request(t.Context(), "contracts", map[string]any{"action": "list", "format": "json"}, f.srv.handleContracts)
	body := requireContractBody(t, result, err)
	fields := contractAnalysisFields(t, body)
	require.Equal(t, "complete", fields["state"])
	require.Equal(t, true, fields["fresh"])
	for range 5 {
		ctx, refusal := prepareConsumerForTest(t.Context(), f, requestFreshness{}, capabilityRequest{})
		require.Nil(t, refusal)
		binding := contractAnalysisFromContext(ctx)
		require.Len(t, binding.views, 2)
		require.NoError(t, binding.checkCohort(), "groups need not share inputs")
		nodes, err := binding.analysisNodes(t.Context(), []graph.NodeKind{graph.KindContract})
		require.NoError(t, err)
		require.Len(t, nodes, 1)
		require.Equal(t, "b/handler.go", nodes[0].FilePath, "the row of the record that sorts first")
		ungrouped := &contractAnalysisContext{views: binding.views}
		require.Error(t, ungrouped.checkCohort(), "one cohort still needs one input vector")
		binding.close()
	}
}

// A split request is one suspended producer waiter while all its groups
// wait, and a repository selector asks only for its own group.
func TestContractWorkspaceGroupsWaitAsOneRequest(t *testing.T) {
	f := newWorkspaceGroupFixture(t, "wa", "wb")
	var schedules groupSchedules
	schedules.install(t, f, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = f.request(ctx, "contracts", map[string]any{"action": "list", "format": "json"}, f.srv.handleContracts)
	}()
	require.Eventually(t, func() bool { return len(schedules.actors()) == 2 }, 10*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return f.srv.contractAnalysisWaiters.Load() == 1 }, 10*time.Second, time.Millisecond)
	cancel()
	<-done
	// A cancelled call returns before its handler and groups unwind.
	require.Eventually(t, func() bool { return f.srv.contractAnalysisWaiters.Load() == 0 }, 10*time.Second, time.Millisecond)

	var targeted groupSchedules
	targeted.install(t, f, nil)
	ctx, cancel = context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	result, err := f.request(ctx, "contracts", map[string]any{"action": "list", "repo": "repo", "format": "json"}, f.srv.handleContracts)
	require.NoError(t, err)
	require.True(t, result.IsError)
	require.Equal(t, map[string][]string{"repo": {"repo"}}, targeted.actors())
}
