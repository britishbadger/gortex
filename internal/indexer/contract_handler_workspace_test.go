package indexer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zzet/gortex/internal/graph"
)

// A route's handler resolves from candidates of its own workspace only: a
// namesake in another workspace neither makes a controller ambiguous nor
// shadows the trail's own-repo handler.
func TestContractHandlerCandidatesStayWithinTheContractWorkspace(t *testing.T) {
	idx := &Indexer{repoPrefix: "a", workspaceID: "wa"}
	controller := func(repo, workspace string) *graph.Node {
		return &graph.Node{ID: repo + "/app/Http/Controllers/UserController.php::UserController", Name: "UserController", Kind: graph.KindType, RepoPrefix: repo, WorkspaceID: workspace, Meta: map[string]any{"scope_ns": `App\Http\Controllers`}}
	}
	action := func(repo, workspace string) *graph.Node {
		return &graph.Node{ID: controller(repo, workspace).ID + ".index", Name: "index", Kind: graph.KindMethod, FilePath: repo + "/app/Http/Controllers/UserController.php", RepoPrefix: repo, WorkspaceID: workspace}
	}
	class := `App\Http\Controllers\UserController`
	byName := map[string][]*graph.Node{"UserController": {controller("a", "wa"), controller("z", "wb")}, "index": {action("a", "wa"), action("z", "wb")}}
	got := idx.resolveInnermostHandlerFromCandidates("[UserController::class, 'index']", "", "a", "a/routes", class, byName)
	require.NotNil(t, got, "another workspace's namesake does not make the controller ambiguous")
	require.Equal(t, action("a", "wa").ID, got.ID)
	byName["UserController"] = append(byName["UserController"], controller("c", "wa"))
	require.Nil(t, idx.resolveInnermostHandlerFromCandidates("[UserController::class, 'index']", "", "a", "a/routes", class, byName), "a namesake in the same workspace still does")

	wrap := &graph.Node{ID: "a/auth.go::WithAuth", Name: "WithAuth", Kind: graph.KindFunction, FilePath: "a/auth.go", RepoPrefix: "a"}
	list := &graph.Node{ID: "z/list.go::H.List", Name: "List", Kind: graph.KindMethod, FilePath: "z/list.go", RepoPrefix: "z"}
	trail := map[string][]*graph.Node{"WithAuth": {wrap}, "List": {list}}
	got = idx.resolveInnermostHandlerFromCandidates("WithAuth(h.List)", "", "a", "a", "", trail)
	require.NotNil(t, got)
	require.Equal(t, wrap.ID, got.ID, "the trail resolves as the workspace alone resolves it")
	idx.contractRepoWorkspaces = map[string]string{"z": "wa"}
	got = idx.resolveInnermostHandlerFromCandidates("WithAuth(h.List)", "", "a", "a", "", trail)
	require.Equal(t, list.ID, got.ID, "a declaration row names its workspace through its repo")
}

// The analysis job resolves a route's handler the same whether or not the
// cohort holds another workspace's repos; a repo of the same workspace still
// takes part.
func TestContractJobHandlersStayWithinTheContractWorkspace(t *testing.T) {
	run := func(other string) any {
		t.Helper()
		f := newContractWorkerFixture(t)
		add := func(repo, workspace, path, source string) {
			addWorkspaceWorkerFile(t, f, repo, workspace, path, source)
		}
		add("repo-a", "workspace", "repo-a/routes.go", `package server
import "net/http"
func routes(mux *http.ServeMux) { mux.HandleFunc("GET /v1/users", WithAuth(List)) }
`)
		add("repo-a", "workspace", "repo-a/auth.go", `package server
import "net/http"
func WithAuth(next http.HandlerFunc) http.HandlerFunc { return next }
`)
		if other != "" {
			add("repo-c", other, "repo-c/list.go", `package other
import "net/http"
func List(w http.ResponseWriter, r *http.Request) {}
`)
		}
		req := f.request(t, "repo-a", "analysis-repo-a")
		report, err := RunContractFollowup(context.Background(), req)
		require.NoError(t, err)
		require.True(t, report.Published)
		node := req.Payload.GetNode("http::GET::/v1/users")
		require.NotNil(t, node)
		return node.Meta["symbol_id"]
	}
	alone := run("")
	require.Equal(t, "repo-a/auth.go::WithAuth", alone)
	require.Equal(t, alone, run("elsewhere"), "another workspace's List does not shadow the route's handler")
	require.Equal(t, "repo-a/routes.go::routes", run("workspace"), "a List of the same workspace still does, as before")
}
