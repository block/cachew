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

// EnsureRefsRequest is the JSON request body for POST .../ensure-refs.
//
// Refs maps each required ref (e.g. "refs/heads/main") to the expected SHA.
// An empty SHA means "require the ref to exist, at any SHA".
type EnsureRefsRequest struct {
	Refs map[string]string `json:"refs"`
}

// EnsureRefsResponse is the JSON response body for POST .../ensure-refs.
//
// Refs contains the resolved local SHA for each requested ref (empty if the
// ref is still missing after the fetch). Fetched reports whether an upstream
// fetch was performed to satisfy the request.
type EnsureRefsResponse struct {
	Refs    map[string]string `json:"refs"`
	Fetched bool              `json:"fetched"`
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
	if len(req.Refs) == 0 {
		http.Error(w, "refs is required and must be non-empty", http.StatusBadRequest)
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

	if repo.State() != gitclone.StateReady {
		if err := s.ensureCloneReady(ctx, repo); err != nil {
			logger.ErrorContext(ctx, "Clone not ready", "error", err, "upstream", upstreamURL)
			http.Error(w, "clone not ready: "+err.Error(), http.StatusBadGateway)
			return
		}
	}

	resolved, fetched, err := repo.EnsureRefs(ctx, req.Refs)
	if err != nil {
		logger.ErrorContext(ctx, "EnsureRefs failed", "error", errors.WithStack(err), "upstream", upstreamURL)
		http.Error(w, "ensure refs: "+err.Error(), http.StatusBadGateway)
		return
	}

	logger.DebugContext(ctx, "EnsureRefs completed", "upstream", upstreamURL,
		"requested", len(req.Refs), "fetched", fetched)

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(EnsureRefsResponse{Refs: resolved, Fetched: fetched}); err != nil {
		logger.ErrorContext(ctx, "Failed to encode response", "error", err)
	}
}
