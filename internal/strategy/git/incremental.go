package git

import (
	"context"
	"net/http"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/gitclone"
	"github.com/block/cachew/internal/logging"
)

// Incremental pull-through outcome values, recorded on the
// incrementalServeTotal counter. local_hit means all wants were already in the
// mirror and fetched means a git fetch brought them in. Every other outcome
// forwards the request upstream.
const (
	incrementalLocalHit            = "local_hit"
	incrementalFetched             = "fetched"
	incrementalFallbackFetchFailed = "fallback_fetch_failed"
	incrementalFallbackMissing     = "fallback_missing"
	// fallback_local_error covers failures of the local missing-object check
	// itself (cat-file errors, dead contexts) so they are not mistaken for
	// upstream fetch failures in metrics.
	incrementalFallbackLocalError = "fallback_local_error"
	// client_gone means the request context was cancelled before the incremental
	// fetch path could complete; it is not an upstream fetch failure.
	incrementalClientGone = "client_gone"
	// client_gone_after_fetch means the client disconnected after the coalescing
	// fetch attempt, which may have succeeded and left the mirror up to date.
	incrementalClientGoneAfterFetch = "client_gone_after_fetch"
	// fallback_not_our_ref means incremental cleared the wants but the local
	// backend still reported "not our ref"/"unknown ref", so the request needed a
	// full upstream passthrough. A separate outcome keeps those passthroughs out
	// of the local_hit/fetched success counts.
	incrementalFallbackNotOurRef = "fallback_not_our_ref"
)

// classifyMissingErr maps a MissingObjects failure to an incremental outcome: a
// dead request context is client_gone, anything else is a local check failure.
func classifyMissingErr(ctx context.Context, err error, wrapMsg string) (string, error) {
	if ctx.Err() != nil {
		return incrementalClientGone, errors.Wrap(ctx.Err(), wrapMsg)
	}
	return incrementalFallbackLocalError, errors.Wrap(err, wrapMsg)
}

// isUploadPackPost reports whether the request is an upload-pack POST, the only
// request shape the incremental path considers, so it is also the denominator
// for the incrementalEligibleTotal counter.
func isUploadPackPost(r *http.Request, pathValue string) bool {
	return r.Method == http.MethodPost && strings.HasSuffix(pathValue, "/git-upload-pack")
}

// incrementalParse extracts the want OIDs from a buffered upload-pack POST body,
// plus whether the body carries protocol v2 want-ref lines. Empty wants means
// the request skips the incremental path (not a fetch, v2 ls-refs, no wants,
// parse failure), because the existing serve path is the safe default. wantRefs
// is reported separately because a want-ref body asks the mirror to resolve ref
// names, which the caller must not trust unless the mirror's refs are known
// fresh.
func incrementalParse(r *http.Request, pathValue string, body []byte) (wants []string, wantRefs bool) {
	if !isUploadPackPost(r, pathValue) {
		return nil, false
	}
	gzipEncoded := strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip")
	req, err := ParseUploadPackRequest(body, gzipEncoded)
	if err != nil {
		logger := logging.FromContext(r.Context())
		if errors.Is(err, errBodyTooLarge) {
			// This is a well-formed fetch we declined, not a malformed body, so
			// log it loudly enough to notice if real clients trip the limit.
			logger.WarnContext(r.Context(), "Skipping incremental path: upload-pack body exceeds parse limit",
				"parse_limit", uploadPackParseLimit, "body_bytes", len(body), "path", pathValue)
			return nil, false
		}
		logger.DebugContext(r.Context(), "Skipping incremental path: upload-pack parse failed", "error", err)
		return nil, false
	}
	if req.LsRefs {
		return nil, false
	}
	return req.Wants, req.WantRefs
}

// ensureWantsAvailable makes sure the mirror contains every wanted object,
// fetching from upstream if needed. Git negotiation means only the missing delta
// crosses the network.
//
// Fetch amplification is bounded by the same RefCheckInterval cooldown that
// freshenMirror uses, not by reasoning about ref namespaces: the staleness check
// compares only refs/heads/*, so a "fresh" result says nothing about a want
// reachable solely from refs/pull/* or refs/tags/*.
//
// The fetch is detached from the request context with WithoutCancel so a client
// disconnect cannot kill an in-flight delta transfer; the mirror still converges
// under IncrementalFetchTimeout. The request context is checked after each fetch
// attempt so a disconnect reports client_gone_after_fetch rather than a fetch or
// local-check failure. MissingObjects re-checks use the request context while it
// is still live, since they are local and fast.
//
// The caller must forward to upstream on any non-nil error or on
// incrementalFallbackMissing, which means the want is still absent: either
// upstream does not have it or the fetch cooldown blocked the attempt. Any other
// outcome may be served locally.
func (s *Strategy) ensureWantsAvailable(ctx context.Context, repo *gitclone.Repository, wants []string) (string, error) {
	missing, err := repo.MissingObjects(ctx, wants)
	if err != nil {
		return classifyMissingErr(ctx, err, "check missing objects")
	}
	if len(missing) == 0 {
		return incrementalLocalHit, nil
	}
	// Cooldown alone is not enough. A recently-completed fetch means retrying
	// would amplify traffic, but an in-flight fetch should be coalesced onto
	// below, not skipped past.
	if !repo.NeedsFetch(s.cloneManager.Config().RefCheckInterval) && !repo.FetchInFlight() {
		return incrementalFallbackMissing, nil
	}

	// Detach from the request context so a client disconnect leaves the delta
	// fetch running to completion under IncrementalFetchTimeout.
	base := context.WithoutCancel(ctx)
	hctx, cancel := context.WithTimeout(base, s.config.IncrementalFetchTimeout)
	defer cancel()
	// Give the coalescing attempt half the budget so the verified attempt still
	// has room if the first call only waited on a holder that did not fetch.
	fctx, fcancel := context.WithTimeout(hctx, s.config.IncrementalFetchTimeout/2)
	defer fcancel()

	// First attempt: coalesces with any in-flight background fetch, so concurrent
	// requests do not each issue a network call for the same delta. The coalesce
	// wait spans the whole fctx budget on purpose: the semaphore serializes
	// fetches anyway, so giving up early cannot start the verified fetch sooner,
	// it would only turn would-be local serves into upstream fallbacks.
	//
	// lastFetchBefore lets a caller that only coalesced (fetched=false) still
	// recognize a real fetch that finished while it waited: fetchSem has capacity
	// 1, so one holder runs at a time, and LastFetch advances only on a
	// successful fetch. Without it, every coalescing waiter on an unresolvable
	// want ran its own guaranteed fetch below, amplifying upstream traffic
	// instead of trusting the fetch it just waited on.
	lastFetchBefore := repo.LastFetch()
	fetched, err := s.doFetchReporting(fctx, repo)
	if err != nil {
		return incrementalFallbackFetchFailed, errors.Wrap(err, "fetch for incremental pull-through")
	}
	if !fetched && repo.LastFetch().After(lastFetchBefore) {
		fetched = true
	}

	// The coalesce wait runs under WithoutCancel, so a client disconnect does not
	// abort it. Classify that case before MissingObjects, which would otherwise
	// report fallback_local_error for a dead request context.
	if err := ctx.Err(); err != nil {
		return incrementalClientGoneAfterFetch, errors.Wrap(err, "client gone after incremental fetch")
	}

	// The re-check runs under the request context, not fctx: a coalesce wait may
	// have consumed most of the fetch budget, and cat-file is local and fast, so
	// a near-dead fctx would only produce spurious failures.
	missing, err = repo.MissingObjects(ctx, wants)
	if err != nil {
		return classifyMissingErr(ctx, err, "re-check missing objects after fetch")
	}
	if len(missing) == 0 {
		return incrementalFetched, nil
	}

	// The want is still missing. If this call ran a git fetch, upstream does not
	// have the object (force-pushed away, say), so a verified retry would only
	// amplify upstream traffic. A guaranteed fetch is worth it only when we
	// coalesced onto another semaphore holder that may not have fetched at all,
	// such as a snapshot tar operation.
	if fetched {
		return incrementalFallbackMissing, nil
	}

	// Second attempt: wait for the semaphore and fetch only if wants are still
	// missing. A peer may have delivered them while this call waited.
	//
	// Re-check the cooldown first. Coalescing onto a fetch that failed also
	// reports fetched=false, so during an upstream outage every waiter would
	// otherwise fall through to its own full fetch attempt, serializing them all
	// behind the semaphore instead of honouring the cooldown the failed attempt
	// just stamped.
	if !repo.NeedsFetch(s.cloneManager.Config().RefCheckInterval) {
		return incrementalFallbackMissing, nil
	}
	if err := s.doFetchVerifiedIfAbsent(hctx, repo, wants); err != nil {
		return incrementalFallbackFetchFailed, errors.Wrap(err, "verified fetch for incremental pull-through")
	}

	// The verified attempt is detached too, so classify a disconnect here rather
	// than letting the re-check below report client_gone and queue a forced fetch
	// the mirror does not need.
	if err := ctx.Err(); err != nil {
		return incrementalClientGoneAfterFetch, errors.Wrap(err, "client gone after verified incremental fetch")
	}

	// Same rationale as the first re-check: use the request context, not the
	// possibly-exhausted fetch budget.
	missing, err = repo.MissingObjects(ctx, wants)
	if err != nil {
		return classifyMissingErr(ctx, err, "re-check missing objects after verified fetch")
	}
	if len(missing) > 0 {
		// The want is not reachable from any upstream ref (e.g. force-pushed away).
		return incrementalFallbackMissing, nil
	}
	return incrementalFetched, nil
}

// incrementalOutcomeAfterNotOurRef reclassifies a successful incremental outcome
// when the local backend still reports "not our ref"/"unknown ref".
func incrementalOutcomeAfterNotOurRef(incrementalOutcome string) string {
	if incrementalOutcome != "" {
		return incrementalFallbackNotOurRef
	}
	return ""
}
