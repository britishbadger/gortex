package mcp

import (
	"context"
	"reflect"
	"sort"
	"sync"

	"github.com/zzet/gortex/internal/contracts"
	"github.com/zzet/gortex/internal/graph"
	"github.com/zzet/gortex/internal/graphview"
	"github.com/zzet/gortex/internal/indexer"
	"github.com/zzet/gortex/internal/query"
)

type contractAnalysisContextKey struct{}

// contractAnalysisContext binds independently leased, complete analysis to one
// RPC. It never replaces readerFor's core source/declaration reader.
type contractAnalysisContext struct {
	views         map[string]*graphview.ContractAnalysisView
	once          sync.Once
	registry      *contracts.Registry
	registryErr   error
	mu            sync.Mutex
	readErr       error
	graphOnce     sync.Once
	graph         graph.Reader
	graphErr      error
	composedOnce  sync.Once
	composed      graph.Reader
	composedLayer *contractIdentityLayer
	composedErr   error
	closeOnce     sync.Once
	releaseCohort func()
	served        bool
	recovered     bool
	lazy          func(context.Context) error
	lazyOnce      sync.Once
	lazyErr       error
	// registries reuses unchanged repository loads across requests; nil
	// loads every repository.
	registries *contractRegistryCache
	// cohortOf names each view's workspace group when a request was split
	// by workspace; nil when every view is of one cohort.
	cohortOf map[string]int
}

func withContractAnalysisContext(ctx context.Context, binding *contractAnalysisContext) context.Context {
	return context.WithValue(ctx, contractAnalysisContextKey{}, binding)
}

func contractAnalysisFromContext(ctx context.Context) *contractAnalysisContext {
	binding, _ := ctx.Value(contractAnalysisContextKey{}).(*contractAnalysisContext)
	return binding
}

// ensure binds an optional component once, only when a handler actually asks
// for contract registry/graph/shape data. The request owns this placeholder's
// cleanup before any lazily opened payloads exist.
func (binding *contractAnalysisContext) ensure(ctx context.Context) error {
	if binding == nil {
		return graph.ErrContractProjectionIncomplete
	}
	binding.lazyOnce.Do(func() {
		if binding.lazy != nil {
			binding.lazyErr = binding.lazy(ctx)
		}
	})
	return binding.lazyErr
}

func (binding *contractAnalysisContext) close() {
	if binding == nil {
		return
	}
	binding.closeOnce.Do(func() {
		for _, view := range binding.views {
			view.Close()
		}
		if binding.releaseCohort != nil {
			binding.releaseCohort()
		}
	})
}

func (binding *contractAnalysisContext) recordReadError(err error) {
	if err == nil {
		return
	}
	binding.mu.Lock()
	if binding.readErr == nil {
		binding.readErr = err
	}
	binding.mu.Unlock()
}

func (binding *contractAnalysisContext) readError() error {
	if binding == nil {
		return nil
	}
	binding.mu.Lock()
	defer binding.mu.Unlock()
	return binding.readErr
}

// Every repo payload participating in a match or bridge must have consulted
// the same selected companion inputs. Independent latest attachments are not
// a coherent cross-repository snapshot, even when their canonical IDs match.
// A request split by workspace holds this within each group: no match or
// bridge spans two workspaces.
func (binding *contractAnalysisContext) checkCohort() error {
	groups := make(map[int][]*graphview.ContractAnalysisView)
	for repo, view := range binding.views {
		groups[binding.cohortOf[repo]] = append(groups[binding.cohortOf[repo]], view)
	}
	for _, views := range groups {
		if err := checkCohortInputs(views); err != nil {
			return err
		}
	}
	return nil
}

func checkCohortInputs(views []*graphview.ContractAnalysisView) error {
	var version, fingerprint string
	for _, view := range views {
		if len(views) <= 1 {
			return nil
		}
		if view == nil || view.Attachment.InputVersion == "" || view.Attachment.InputFingerprint == "" {
			return graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "selected contract companion inputs are unavailable")
		}
		if version == "" {
			version, fingerprint = view.Attachment.InputVersion, view.Attachment.InputFingerprint
		} else if version != view.Attachment.InputVersion || fingerprint != view.Attachment.InputFingerprint {
			return graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "contract analyses consulted different selected companion inputs")
		}
	}
	return nil
}

func (binding *contractAnalysisContext) validate(ctx context.Context) error {
	if binding != nil && binding.lazyErr != nil {
		return binding.lazyErr
	}
	if err := binding.readError(); err != nil {
		return err
	}
	if binding == nil || len(binding.views) == 0 {
		return graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "selected contract analysis is unavailable")
	}
	if err := binding.checkCohort(); err != nil {
		return err
	}
	for _, view := range binding.views {
		var err error
		if binding.served {
			err = view.ValidateServed(ctx)
		} else {
			err = view.Validate(ctx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (binding *contractAnalysisContext) loadRegistry(ctx context.Context) (*contracts.Registry, error) {
	if err := binding.ensure(ctx); err != nil {
		return nil, err
	}
	binding.once.Do(func() {
		binding.registry = contracts.NewRegistry()
		if len(binding.views) == 0 {
			binding.registryErr = graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "selected contract analysis is unavailable")
			return
		}
		if err := binding.checkCohort(); err != nil {
			binding.registryErr = err
			return
		}
		repos := make([]string, 0, len(binding.views))
		for repo := range binding.views {
			repos = append(repos, repo)
		}
		sort.Strings(repos)
		for _, repo := range repos {
			view := binding.views[repo]
			reg, err := binding.registries.loadRepo(ctx, view, repo)
			if err != nil {
				binding.registryErr = err
				return
			}
			// nil,nil is a complete empty snapshot, not an unbuilt registry.
			if reg == nil {
				continue
			}
			for _, id := range reg.AllIDs() {
				for _, record := range reg.ByID(id) {
					binding.registry.Add(record)
				}
			}
		}
	})
	if binding.registryErr != nil {
		// An opened attachment's checked projection failure is sticky even when
		// an optional handler omits its contract section. Absence has no opened
		// payload and remains a valid default core-only answer.
		if len(binding.views) > 0 {
			binding.recordReadError(binding.registryErr)
		}
		return nil, binding.registryErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return binding.registry, nil
}

// contractRegistryForContext is the common consumer seam. A routed checkout
// may never borrow the primary registry when no selected analysis was bound.
// Legacy unrouted callers remain unchanged until the producer is enabled.
func (s *Server) contractRegistryForContext(ctx context.Context) (*contracts.Registry, error) {
	if binding := contractAnalysisFromContext(ctx); binding != nil {
		return binding.loadRegistry(ctx)
	}
	if requestViewFromContext(ctx).routed() {
		return nil, graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "contract analysis is not bound to the selected view")
	}
	return s.effectiveContractRegistry(), nil
}

// Optional consumers bind lazily at this first registry read. An unavailable
// attachment omits the contract section after recording interest; a routed
// request never borrows the primary registry.
func (s *Server) optionalContractRegistryForContext(ctx context.Context) *contracts.Registry {
	registry, err := s.contractRegistryForContext(ctx)
	if err != nil {
		return nil
	}
	return registry
}

func (s *Server) contractReaderForContext(ctx context.Context) (graph.Reader, error) {
	if binding := contractAnalysisFromContext(ctx); binding != nil {
		return binding.analysisReader(ctx)
	}
	return s.readerFor(ctx), nil
}

func (binding *contractAnalysisContext) shapeLookup(ctx context.Context) contracts.ShapeLookup {
	if err := binding.ensure(ctx); err != nil {
		binding.recordReadError(err)
		return func(string) *contracts.Shape { return nil }
	}
	repos := make([]string, 0, len(binding.views))
	for repo := range binding.views {
		repos = append(repos, repo)
	}
	sort.Strings(repos)
	return func(id string) *contracts.Shape {
		var found *contracts.Shape
		for _, repo := range repos {
			node, err := binding.views[repo].ShapeNode(ctx, id)
			if err != nil {
				binding.recordReadError(err)
				return nil
			}
			if node == nil || node.Meta == nil {
				continue
			}
			var shape *contracts.Shape
			switch value := node.Meta["shape"].(type) {
			case *contracts.Shape:
				shape = value
			case contracts.Shape:
				shape = &value
			}
			if shape == nil {
				continue
			}
			if found != nil && !reflect.DeepEqual(found, shape) {
				binding.recordReadError(graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "schema identity is ambiguous across selected contract analyses"))
				return nil
			}
			found = shape
		}
		return found
	}
}

func (binding *contractAnalysisContext) analysisNodes(ctx context.Context, kinds []graph.NodeKind) ([]*graph.Node, error) {
	ambiguous := func() error {
		err := graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "contract node identity is ambiguous across selected analyses")
		binding.recordReadError(err)
		return err
	}
	groups := make(map[int]map[string]*graph.Node)
	for repo, view := range binding.views {
		var nodes []*graph.Node
		var err error
		if kinds == nil {
			nodes, err = view.NodesContext(ctx, graph.ContractProjectionRowLimit)
		} else {
			nodes, err = view.NodesByKindsContext(ctx, kinds)
		}
		if err != nil {
			binding.recordReadError(err)
			return nil, err
		}
		group := groups[binding.cohortOf[repo]]
		if group == nil {
			group = make(map[string]*graph.Node)
			groups[binding.cohortOf[repo]] = group
		}
		for _, node := range nodes {
			if old := group[node.ID]; old != nil && !reflect.DeepEqual(old, node) {
				return nil, ambiguous()
			}
			group[node.ID] = node
		}
	}
	// Workspace groups each write a contract ID's row from their own records;
	// the row a single cohort writes is the one whose record sorts first.
	order := make([]int, 0, len(groups))
	for group := range groups {
		order = append(order, group)
	}
	sort.Ints(order)
	byID := make(map[string]*graph.Node)
	for _, group := range order {
		for id, node := range groups[group] {
			old := byID[id]
			switch {
			case old == nil:
				byID[id] = node
			case reflect.DeepEqual(old, node):
			case old.Kind == graph.KindContract && node.Kind == graph.KindContract:
				if indexer.ContractNodeRowLess(node, old) {
					byID[id] = node
				}
			default:
				return nil, ambiguous()
			}
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	nodes := make([]*graph.Node, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, byID[id])
	}
	return nodes, nil
}

func (binding *contractAnalysisContext) analysisOutEdges(ctx context.Context, ids []string) (map[string][]*graph.Edge, error) {
	combined := make(map[string][]*graph.Edge)
	seen := make(map[graph.EdgeIdentity]*graph.Edge)
	for _, view := range binding.views {
		edges, err := view.OutEdgesContext(ctx, ids, graph.ContractProjectionRowLimit)
		if err != nil {
			binding.recordReadError(err)
			return nil, err
		}
		for id, rows := range edges {
			for _, edge := range rows {
				identity := graph.EdgeIdentityFor(edge)
				if old := seen[identity]; old != nil {
					if !reflect.DeepEqual(old, edge) {
						err := graphview.NewViewError(graphview.CodeRequiredCapabilityIncomplete, "contract edge identity is ambiguous across selected analyses")
						binding.recordReadError(err)
						return nil, err
					}
					continue
				}
				seen[identity] = edge
				combined[id] = append(combined[id], edge)
			}
		}
	}
	return combined, nil
}

// analysisReader builds a request-local graph solely from checked analysis
// rows. It never embeds the core reader's legacy contract edges. Current core
// source identities are added only as evidence for owned contract endpoints.
func (binding *contractAnalysisContext) analysisReader(ctx context.Context) (graph.Reader, error) {
	if err := binding.ensure(ctx); err != nil {
		return nil, err
	}
	if len(binding.views) == 0 {
		return nil, graph.ErrContractProjectionIncomplete
	}
	binding.graphOnce.Do(func() {
		nodes, err := binding.analysisNodes(ctx, nil)
		if err != nil {
			binding.graphErr = err
			return
		}
		byID := make(map[string]*graph.Node, len(nodes))
		for _, node := range nodes {
			byID[node.ID] = node
		}
		for repo, view := range binding.views {
			projection, err := view.RegistryReader.(graph.ContractRepoProjectionReader).LoadContractRepoProjectionContext(ctx, repo)
			if err != nil {
				binding.graphErr = err
				return
			}
			for id, node := range projection.SourceNodes {
				byID[id] = node
			}
		}
		ids := make([]string, 0, len(byID))
		for id := range byID {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		edges, err := binding.analysisOutEdges(ctx, ids)
		if err != nil {
			binding.graphErr = err
			return
		}
		var missing []string
		missingSet := make(map[string]bool)
		for _, rows := range edges {
			for _, edge := range rows {
				if byID[edge.To] == nil && !missingSet[edge.To] {
					missingSet[edge.To] = true
					missing = append(missing, edge.To)
				}
			}
		}
		for _, view := range binding.views {
			physical, err := view.Layer.LayerContractIDProjectionContext(ctx, missing)
			if err != nil {
				binding.graphErr = err
				return
			}
			for id, node := range physical.SourceNodes {
				byID[id] = node
			}
		}
		for _, id := range missing {
			if byID[id] == nil {
				binding.graphErr = graph.ErrContractProjectionIncomplete
				return
			}
		}
		analysis := graph.NewOverlayLayer()
		for _, node := range byID {
			analysis.AddNode(node.FilePath, node)
		}
		for _, rows := range edges {
			for _, edge := range rows {
				analysis.AddEdge(edge)
			}
		}
		// The overlay's iterators enumerate layer rows only with a non-nil
		// baseline. An explicit empty view keeps analysis isolated while
		// allowing bridge/framework and candidate enumeration.
		binding.graph = graph.NewOverlaidViewWithLayer(graph.NewOverlaidViewWithLayer(nil, nil), analysis)
	})
	if binding.graphErr != nil {
		binding.recordReadError(binding.graphErr)
		return nil, binding.graphErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return binding.graph, nil
}

// scopeEvidence retains canonical identity while applying selectors to actual
// selected owners. A shared global canonical row need not carry each owner's
// repository or source path in its own scalar fields.
func (binding *contractAnalysisContext) scopeEvidence(ctx context.Context, node *graph.Node) []*graph.Node {
	if node == nil {
		return nil
	}
	if node.Kind != graph.KindContract && node.Kind != graph.KindContractBridge && node.Kind != graph.KindConfigKey {
		return []*graph.Node{node}
	}
	id := node.ID
	if node.Kind == graph.KindContractBridge {
		id, _ = node.Meta["contract_id"].(string)
	}
	registry, err := binding.loadRegistry(ctx)
	if err != nil {
		binding.recordReadError(err)
		return nil
	}
	ids := []string{id}
	if node.Kind == graph.KindContractBridge {
		// RPC/service bridges may group several distinct literal contract
		// IDs; their actual selected links are the membership authority.
		reader, err := binding.analysisReader(ctx)
		if err != nil {
			binding.recordReadError(err)
			return nil
		}
		for _, edge := range reader.GetOutEdges(node.ID) {
			if edge.Kind == graph.EdgeBridges {
				ids = append(ids, edge.To)
			}
		}
	}
	var evidence []*graph.Node
	for _, id := range ids {
		for _, owner := range registry.ByID(id) {
			copyNode := *node
			copyNode.RepoPrefix, copyNode.FilePath = owner.RepoPrefix, owner.FilePath
			copyNode.WorkspaceID, copyNode.ProjectID = owner.WorkspaceID, owner.ProjectID
			evidence = append(evidence, &copyNode)
		}
	}
	if node.Kind == graph.KindConfigKey {
		reader, err := binding.analysisReader(ctx)
		if err != nil {
			binding.recordReadError(err)
			return nil
		}
		for _, edge := range reader.GetInEdges(node.ID) {
			if edge.Kind != graph.EdgeReadsConfig {
				continue
			}
			if source := reader.GetNode(edge.From); source != nil {
				copyNode := *node
				copyNode.RepoPrefix, copyNode.FilePath = source.RepoPrefix, source.FilePath
				copyNode.WorkspaceID, copyNode.ProjectID = source.WorkspaceID, source.ProjectID
				evidence = append(evidence, &copyNode)
			}
		}
	}
	return evidence
}

func (s *Server) contractScopeAllowsNode(ctx context.Context, scope ResolvedScope, node *graph.Node) bool {
	binding := contractAnalysisFromContext(ctx)
	status := contractConsumerStatusFromContext(ctx)
	if binding == nil || status == nil || status.mode != contractConsumerRequired {
		return resolvedScopeAllowsNode(scope, node)
	}
	for _, evidence := range binding.scopeEvidence(ctx, node) {
		if resolvedScopeAllowsNode(scope, evidence) {
			return true
		}
	}
	return false
}

func (s *Server) filterContractSubGraph(ctx context.Context, sg *query.SubGraph, allowed map[string]bool) *query.SubGraph {
	binding := contractAnalysisFromContext(ctx)
	status := contractConsumerStatusFromContext(ctx)
	if binding == nil || status == nil || status.mode != contractConsumerRequired || allowed == nil {
		return filterSubGraph(sg, allowed)
	}
	copyGraph := *sg
	copyGraph.Nodes = nil
	copyGraph.Edges = nil
	kept := make(map[string]bool)
	for _, node := range sg.Nodes {
		for _, evidence := range binding.scopeEvidence(ctx, node) {
			if repoNarrowAdmits(allowed, evidence.RepoPrefix) {
				copyGraph.Nodes = append(copyGraph.Nodes, node)
				kept[node.ID] = true
				break
			}
		}
	}
	for _, edge := range sg.Edges {
		if kept[edge.From] || kept[edge.To] {
			copyGraph.Edges = append(copyGraph.Edges, edge)
		}
	}
	copyGraph.TotalNodes = len(copyGraph.Nodes)
	if len(sg.Edges) > 0 || sg.TotalEdges == 0 {
		copyGraph.TotalEdges = len(copyGraph.Edges)
	}
	return &copyGraph
}
