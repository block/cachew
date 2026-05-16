package git

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
)

// EnsureRefsPath is the URL suffix for the ref-freshness endpoint. Clients
// POST to /git/{host}/{repo}/ensure-refs to force cachew to ensure the local
// mirror contains the listed refs before they fetch.
const EnsureRefsPath = "/ensure-refs"

// EnsureRefsRequest is the JSON request body for POST .../ensure-refs. At
// least one of Refs or Commits must be non-empty.
//
// Refs maps each required ref (e.g. "refs/heads/main") to the expected SHA.
// An empty SHA means "require the ref to exist, at any SHA".
//
// Commits lists individual commit SHAs that must exist in the mirror's
// object database, regardless of which ref points at them.
type EnsureRefsRequest struct {
	Refs    map[string]string `json:"refs,omitempty"`
	Commits []string          `json:"commits,omitempty"`
}

// EnsureRefsResponse is the JSON response body for POST .../ensure-refs.
//
// Refs contains the resolved local SHA for each requested ref (empty if the
// ref is still missing after the fetch). MissingCommits lists the requested
// commits that are still absent from the local object database after the
// fetch. Fetched reports whether an upstream fetch was performed.
type EnsureRefsResponse struct {
	Refs           map[string]string `json:"refs,omitempty"`
	MissingCommits []string          `json:"missing_commits,omitempty"`
	Fetched        bool              `json:"fetched"`
}

func (s *Strategy) handleEnsureRefs(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	ctx := r.Context()
	logger := logging.FromContext(ctx)

	s.metrics.recordRequest(ctx, "ensure-refs")

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req EnsureRefsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Refs) == 0 && len(req.Commits) == 0 {
		http.Error(w, "at least one of refs or commits must be provided", http.StatusBadRequest)
		return
	}

	repoPath := strings.TrimSuffix(pathValue, EnsureRefsPath)
	repoPath = strings.TrimSuffix(repoPath, ".git")
	upstreamURL := "https://" + host + "/" + repoPath

	repo, err := s.cloneManager.GetOrCreate(ctx, upstreamURL)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get or create clone", "error", err, "upstream", upstreamURL)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.touchRepo(repo)

	if repo.State() != gitclone.StateReady {
		if err := s.ensureCloneReady(ctx, repo); err != nil {
			logger.ErrorContext(ctx, "Clone not ready", "error", err, "upstream", upstreamURL)
			http.Error(w, "clone not ready: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	resolved, missingCommits, fetched, err := repo.EnsureRefs(ctx, req.Refs, req.Commits)
	if err != nil {
		logger.ErrorContext(ctx, "EnsureRefs failed", "error", errors.WithStack(err), "upstream", upstreamURL)
		http.Error(w, "ensure refs: "+err.Error(), http.StatusBadGateway)
		return
	}

	logger.DebugContext(ctx, "EnsureRefs completed", "upstream", upstreamURL,
		"requested_refs", len(req.Refs), "requested_commits", len(req.Commits),
		"missing_commits", len(missingCommits), "fetched", fetched)

	w.Header().Set("Content-Type", "application/json")
	resp := EnsureRefsResponse{Refs: resolved, MissingCommits: missingCommits, Fetched: fetched}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		logger.ErrorContext(ctx, "Failed to encode response", "error", err)
	}
}
