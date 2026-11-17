package git

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/metadatadb"
)

const (
	repoCountsMapName = "repo_clone_counts"
	// '|' cannot appear in a Git upstream URL, so the split is unambiguous.
	repoCountsKeySeparator         = "|"
	defaultRepoCountsRetentionDays = 90
	defaultRepoCountsReapInterval  = 24 * time.Hour
)

// RepoCounts tracks per-repository clone counts in a daily-bucketed IntMap; all methods are nil-safe.
type RepoCounts struct {
	counts        *metadatadb.IntMap[string]
	now           func() time.Time
	retentionDays int
}

// NewRepoCounts returns nil if ns is nil, so callers need no separate "no metadata" path.
func NewRepoCounts(ns *metadatadb.Namespace) *RepoCounts {
	if ns == nil {
		return nil
	}
	return &RepoCounts{
		counts:        metadatadb.NewIntMap[string](ns, repoCountsMapName),
		now:           time.Now,
		retentionDays: defaultRepoCountsRetentionDays,
	}
}

// IncrementClone bumps today's bucket (UTC) for upstreamURL.
func (r *RepoCounts) IncrementClone(upstreamURL string) error {
	if r == nil || upstreamURL == "" {
		return nil
	}
	return errors.Wrap(r.counts.Add(repoCountsKey(upstreamURL, r.now()), 1), "increment repo count")
}

// uploadPackBodyInspectLimit bounds CPU spend on hostile bodies; real ones, even monorepo fetches, are well under it.
const uploadPackBodyInspectLimit = 64 * 1024

// lsRefsLookahead caps the prefix scanned for v2 command=ls-refs, which is always in the first pkt-line.
const lsRefsLookahead = 256

//nolint:gochecknoglobals // hoisted to avoid per-request []byte allocation
var (
	lsRefsNeedle = []byte("command=ls-refs")
	haveNeedle   = []byte("have ")
)

// RequestIsClone reports whether r is an initial clone of a Git repo.
//
// A clone is a POST /git-upload-pack with no "have <oid>" line that is not v2 command=ls-refs. The inspected body
// prefix is replayed so the caller can still read the whole body.
func RequestIsClone(pathValue string, r *http.Request) (bool, error) {
	if r.Method != http.MethodPost || !strings.HasSuffix(pathValue, "/git-upload-pack") {
		return false, nil
	}
	if r.Body == nil || r.Body == http.NoBody {
		return true, nil
	}
	inspect, err := inspectUploadPackBody(r)
	if err != nil {
		return false, err
	}
	if isLsRefsBody(inspect) {
		return false, nil
	}
	// Trailing space disambiguates from capability tokens and repo names.
	if bytes.Contains(inspect, haveNeedle) {
		return false, nil
	}
	return true, nil
}

func requestIsLsRefs(pathValue string, r *http.Request) (bool, error) {
	if r.Method != http.MethodPost || !strings.HasSuffix(pathValue, "/git-upload-pack") ||
		r.Body == nil || r.Body == http.NoBody {
		return false, nil
	}
	inspect, err := inspectUploadPackBody(r)
	if err != nil {
		return false, err
	}
	return isLsRefsBody(inspect), nil
}

func inspectUploadPackBody(r *http.Request) ([]byte, error) {
	prefix := make([]byte, uploadPackBodyInspectLimit)
	n, err := io.ReadFull(r.Body, prefix)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, errors.Wrap(err, "read upload-pack body")
	}
	prefix = prefix[:n]
	// Replay the prefix ahead of the untouched rest, so ContentLength stays correct.
	original := r.Body
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(prefix), original), original}

	inspect := prefix
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		if gr, gzErr := gzip.NewReader(bytes.NewReader(prefix)); gzErr == nil {
			decoded, _ := io.ReadAll(io.LimitReader(gr, uploadPackBodyInspectLimit)) //nolint:errcheck // best-effort
			_ = gr.Close()                                                           //nolint:errcheck // best-effort
			inspect = decoded
		}
	}
	return inspect, nil
}

func isLsRefsBody(inspect []byte) bool {
	head := inspect
	if len(head) > lsRefsLookahead {
		head = head[:lsRefsLookahead]
	}
	return bytes.Contains(head, lsRefsNeedle)
}

// RepoCount is one row of the histogram.
type RepoCount struct {
	Repo  string `json:"repo"`
	Count int64  `json:"count"`
}

// TopRepos sums buckets over the last windowDays days (UTC), sorted by count descending then repo name.
//
// windowDays <= 0 means no window; limit <= 0 means no truncation.
func (r *RepoCounts) TopRepos(windowDays, limit int) []RepoCount {
	if r == nil {
		return nil
	}
	entries := r.counts.Entries()
	var cutoff time.Time
	if windowDays > 0 {
		cutoff = r.now().UTC().AddDate(0, 0, -windowDays+1).Truncate(24 * time.Hour)
	}
	agg := make(map[string]int64, len(entries))
	for k, v := range entries {
		repo, day, ok := splitRepoCountsKey(k)
		if !ok {
			continue
		}
		if !cutoff.IsZero() && day.Before(cutoff) {
			continue
		}
		agg[repo] += v
	}
	out := make([]RepoCount, 0, len(agg))
	for repo, count := range agg {
		out = append(out, RepoCount{Repo: repo, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Repo < out[j].Repo
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// Reap deletes buckets older than the retention window and malformed keys, returning how many it deleted.
func (r *RepoCounts) Reap() (int, error) {
	if r == nil {
		return 0, nil
	}
	entries := r.counts.Entries()
	if len(entries) == 0 {
		return 0, nil
	}
	cutoff := r.now().UTC().AddDate(0, 0, -r.retentionDays).Truncate(24 * time.Hour)
	var deleted int
	for k := range entries {
		_, day, ok := splitRepoCountsKey(k)
		if !ok {
			if err := r.counts.Delete(k); err != nil {
				return deleted, errors.Wrap(err, "delete malformed repo count")
			}
			deleted++
			continue
		}
		if day.Before(cutoff) {
			if err := r.counts.Delete(k); err != nil {
				return deleted, errors.Wrap(err, "delete stale repo count")
			}
			deleted++
		}
	}
	return deleted, nil
}

func repoCountsKey(upstreamURL string, now time.Time) string {
	return upstreamURL + repoCountsKeySeparator + now.UTC().Format("2006-01-02")
}

func splitRepoCountsKey(k string) (repo string, day time.Time, ok bool) {
	idx := strings.LastIndex(k, repoCountsKeySeparator)
	if idx < 0 {
		return "", time.Time{}, false
	}
	repo = k[:idx]
	dateStr := k[idx+1:]
	if repo == "" || dateStr == "" {
		return "", time.Time{}, false
	}
	d, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return "", time.Time{}, false
	}
	return repo, d, true
}
