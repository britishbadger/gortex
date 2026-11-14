package indexer

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/zzet/gortex/internal/contracts"
	"github.com/zzet/gortex/internal/graph"
)

// workspaceTypeStore answers type names from three repos: a and c share
// workspace wa, b is alone in wb.
func workspaceTypeStore() *contractBatchStore {
	return &contractBatchStore{nodesByName: map[string][]*graph.Node{
		"User":    {{ID: "b/model.go::User", Name: "User", Kind: graph.KindType, RepoPrefix: "b", WorkspaceID: "wb"}},
		"Account": {{ID: "c/model.go::Account", Name: "Account", Kind: graph.KindType, RepoPrefix: "c", WorkspaceID: "wa"}},
		"Order":   {{ID: "d/model.go::Order", Name: "Order", Kind: graph.KindType, RepoPrefix: "d"}},
	}}
}

// A bare request or response type resolves to another repo's definition only
// within the contract's own effective workspace, the boundary the matcher
// pairs within; a repo with no workspace is its own.
func TestContractBareTypeRefsStayWithinTheContractWorkspace(t *testing.T) {
	idx := &Indexer{graph: workspaceTypeStore(), repoPrefix: "a", workspaceID: "wa", logger: zap.NewNop()}
	reg := contracts.NewRegistry()
	reg.Add(contracts.Contract{ID: "http::GET::/users", RepoPrefix: "a", WorkspaceID: "wa", Meta: map[string]any{"response_type": "User", "request_type": "Account"}})
	reg.Add(contracts.Contract{ID: "http::GET::/orders", RepoPrefix: "a", WorkspaceID: "wa", Meta: map[string]any{"response_type": "Order"}})
	idx.upgradeContractBareTypeRefs(reg)
	users := reg.ByID("http::GET::/users")[0]
	require.Equal(t, "User", users.Meta["response_type"], "another workspace's type is not the contract's")
	require.Equal(t, "c/model.go::Account", users.Meta["request_type"], "a type of the same workspace still resolves")
	require.Equal(t, "Order", reg.ByID("http::GET::/orders")[0].Meta["response_type"], "a repo with no workspace is its own")
}

func TestContractCallReturnTypeNamesStayWithinTheContractWorkspace(t *testing.T) {
	idx := &Indexer{graph: workspaceTypeStore(), repoPrefix: "a", workspaceID: "wa", logger: zap.NewNop()}
	user := contractTypeNameKey{typeName: "User", repoPrefix: "a"}
	account := contractTypeNameKey{typeName: "Account", repoPrefix: "a"}
	got := idx.upgradeBareTypeNames([]contractTypeNameKey{user, account})
	require.Equal(t, "User", got[user], "another workspace's type is not the contract's")
	require.Equal(t, "c/model.go::Account", got[account], "a type of the same workspace still resolves")
}

// The analysis job reads every cohort repo through one evidence store: a
// cohort spanning workspaces resolves its types as the workspace alone would.
func TestContractJobBareTypesStayWithinTheContractWorkspace(t *testing.T) {
	f := newContractWorkerFixture(t)
	add := func(repo, workspace, path, source string) {
		addWorkspaceWorkerFile(t, f, repo, workspace, path, source)
	}
	add("repo-a", "workspace", "repo-a/users.go", `package server
import "net/http"
func users(mux *http.ServeMux) { mux.HandleFunc("GET /v1/users", listUsers); mux.HandleFunc("GET /v1/accounts", listAccounts) }
func listUsers(w http.ResponseWriter, r *http.Request) { response := User{}; WriteJSON(w,200,response) }
func listAccounts(w http.ResponseWriter, r *http.Request) { response := Account{}; WriteJSON(w,200,response) }
`)
	add("repo-b", "workspace", "repo-b/account.go", "package client\ntype Account struct { ID string `json:\"id\"` }\n")
	add("repo-c", "elsewhere", "repo-c/user.go", "package other\ntype User struct { Name string `json:\"name\"` }\n")
	req := f.request(t, "repo-a", "analysis-repo-a")
	report, err := RunContractFollowup(context.Background(), req)
	require.NoError(t, err)
	require.True(t, report.Published)
	responseType := func(id string) any {
		t.Helper()
		node := req.Payload.GetNode(id)
		require.NotNil(t, node, id)
		meta, _ := node.Meta["contract_meta"].(map[string]any)
		return meta["response_type"]
	}
	require.Equal(t, "User", responseType("http::GET::/v1/users"), "another workspace's type is not the contract's")
	require.Equal(t, "repo-b/account.go::Account", responseType("http::GET::/v1/accounts"), "a type of the same workspace still resolves")
}

// The full index upgrades an inlined contract's bare request, response and
// envelope types within the contract's workspace, as the analysis job does.
func TestContractInlinedBareTypesStayWithinTheContractWorkspace(t *testing.T) {
	g := graph.New()
	g.AddBatch([]*graph.Node{
		{ID: "b/model.go::User", Name: "User", Kind: graph.KindType, FilePath: "b/model.go", RepoPrefix: "b", WorkspaceID: "wb"},
		{ID: "c/model.go::Account", Name: "Account", Kind: graph.KindType, FilePath: "c/model.go", RepoPrefix: "c"},
	}, nil)
	mi := &MultiIndexer{graph: g, logger: zap.NewNop(), indexers: map[string]*Indexer{
		"a": {repoPrefix: "a", workspaceID: "wa", contractRegistry: contracts.NewRegistry()},
		"b": {repoPrefix: "b", workspaceID: "wb", contractRegistry: contracts.NewRegistry()},
		"c": {repoPrefix: "c", workspaceID: "wa", contractRegistry: contracts.NewRegistry()},
	}}
	envelope := func() []map[string]any {
		return []map[string]any{{"expr": "u", "type": "User"}, {"expr": "acct", "type": "Account"}}
	}
	mi.persistScopedInlinedContracts([]contracts.Contract{{ID: "http::GET::/users", Role: contracts.RoleConsumer, Type: contracts.ContractHTTP, RepoPrefix: "a", WorkspaceID: "wa", FilePath: "a/client.go", SymbolID: "a/client.go::fetch", Meta: map[string]any{"response_type": "User", "request_type": "Account", "response_envelope": envelope()}}})
	got := mi.indexers["a"].ContractRegistry().ByID("http::GET::/users")
	require.Len(t, got, 1)
	require.Equal(t, "User", got[0].Meta["response_type"], "another workspace's type is not the contract's")
	require.Equal(t, "c/model.go::Account", got[0].Meta["request_type"], "an unstamped type of the same workspace still resolves")
	rows, _ := got[0].Meta["response_envelope"].([]map[string]any)
	require.Len(t, rows, 2)
	require.Equal(t, "User", rows[0]["type"], "an envelope row stays bare too")
	require.Equal(t, "c/model.go::Account", rows[1]["type"])
}

// Import disambiguation chooses only among the types the contract may name:
// an import that reaches another workspace's repo names no type of its own.
func TestContractImportDisambiguationStaysWithinTheContractWorkspace(t *testing.T) {
	mi := &MultiIndexer{indexers: map[string]*Indexer{"a": {repoPrefix: "a", workspaceID: "wa"}, "z": {repoPrefix: "z", workspaceID: "wb"}}}
	srcCache := map[string][]byte{
		"a/src/client.ts": []byte("import { User } from '../../z/src/user';\n"),
		"z/src/user.ts":   []byte("export interface User { id: string }\n"),
	}
	candidates := map[string][]*graph.Node{"User": {
		{ID: "z/src/user.ts::User", Name: "User", Kind: graph.KindInterface, FilePath: "z/src/user.ts", RepoPrefix: "z"},
		{ID: "z/src/other.ts::User", Name: "User", Kind: graph.KindInterface, FilePath: "z/src/other.ts", RepoPrefix: "z"},
	}}
	require.Equal(t, "z/src/user.ts::User", mi.resolveBareTypeViaImportsPrefetched("a/src/client.ts", "User", srcCache, map[string]map[string]string{}, candidates, nil), "unbounded, the import picks z's type")
	visible := func(n *graph.Node) bool { return mi.contractTypeVisibleLocked(n, "a") }
	require.Empty(t, mi.resolveBareTypeViaImportsPrefetched("a/src/client.ts", "User", srcCache, map[string]map[string]string{}, candidates, visible))
}

// RepoWorkspaceGroups splits repos by effective workspace with the rule
// ReposInWorkspace uses: a repo with no slug, or no indexer, is its own.
func TestRepoWorkspaceGroupsSplitsByEffectiveWorkspace(t *testing.T) {
	mi := &MultiIndexer{indexers: map[string]*Indexer{
		"a": {repoPrefix: "a", workspaceID: "wa"},
		"b": {repoPrefix: "b", workspaceID: "wb"},
		"c": {repoPrefix: "c", workspaceID: "wa"},
		"d": {repoPrefix: "d"},
	}}
	want := [][]string{{"a", "c"}, {"b"}, {"d"}, {"e"}}
	require.Equal(t, want, mi.RepoWorkspaceGroups([]string{"e", "c", "d", "b", "a", "c"}))
	require.Equal(t, want, mi.RepoWorkspaceGroups([]string{"a", "b", "c", "d", "e"}), "the order of repos does not matter")
	require.Equal(t, [][]string{{"a", "c"}}, mi.RepoWorkspaceGroups([]string{"c", "a"}))
	require.Empty(t, mi.RepoWorkspaceGroups(nil))
}

// addWorkspaceWorkerFile indexes one Go file of repo, in workspace, into the
// worker fixture, accepting the repo's inputs the first time it is seen.
func addWorkspaceWorkerFile(t *testing.T, f *contractWorkerFixture, repo, workspace, path, source string) {
	t.Helper()
	f.sources[path] = []byte(source)
	extractor, _ := f.registry.GetByLanguage("go")
	parsed, err := extractor.Extract(filepath.Base(path), f.sources[path])
	require.NoError(t, err)
	if parsed.Tree != nil {
		defer parsed.Tree.Release()
	}
	stripTypeShape(parsed)
	idx := New(graph.New(), f.registry, f.cfg, zap.NewNop())
	defer idx.Close()
	idx.repoPrefix, idx.workspaceID, idx.projectID = repo, workspace, "project"
	idx.applyRepoPrefix(parsed.Nodes, parsed.Edges)
	for _, node := range parsed.Nodes {
		node.WorkspaceID, node.ProjectID = workspace, "project"
	}
	require.NoError(t, f.store.AddBatchChecked(parsed.Nodes, parsed.Edges))
	policy, err := contractFollowupPolicy(idx, "go")
	require.NoError(t, err)
	f.files = append(f.files, ContractFollowupFile{RepoPrefix: repo, WorkspaceID: workspace, ProjectID: "project", Path: path, Language: "go", SourceFingerprint: contractInputHash(f.sources[path]), Policy: policy})
	for _, input := range f.inputs {
		if input.State.RepoPrefix == repo {
			return
		}
	}
	state := graph.ContractInputState{RepoPrefix: repo, InputVersion: "worker-boundary-v1", InputFingerprint: repo + "-accepted"}
	work := graph.ContractWork{Token: repo + "-token", RepoPrefix: repo, FilePath: path, InputVersion: state.InputVersion, InputFingerprint: state.InputFingerprint, State: graph.ContractWorkPending}
	require.NoError(t, f.store.BeginContractInputMutationContext(context.Background(), nil, state, []graph.ContractWork{work}))
	require.NoError(t, f.store.AcceptContractInputMutationContext(context.Background(), state))
	state.Accepted = true
	f.inputs = append(f.inputs, graph.ContractInputWitness{State: state, Found: true})
	f.work[repo] = work
}
