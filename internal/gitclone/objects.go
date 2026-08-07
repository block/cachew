package gitclone

import (
	"bytes"
	"context"
	"os/exec"
	"strings"

	"github.com/alecthomas/errors"
)

// MissingObjects reports which of the given object IDs are absent from the
// repository's local object database, using a single `git cat-file
// --batch-check` invocation. Object IDs must be 40- or 64-character lower-case
// hex; anything else returns an error. The returned slice preserves the input
// order of missing IDs (deduplicated). It takes the repository read lock only
// to serialize against operations that hold the write lock; git fetch runs
// without that lock (see fetchInternal), so the result is a point-in-time
// snapshot that a concurrent fetch may invalidate by adding objects.
func (r *Repository) MissingObjects(ctx context.Context, oids []string) ([]string, error) {
	// Validate and deduplicate preserving order.
	seen := make(map[string]bool, len(oids))
	deduped := make([]string, 0, len(oids))
	for _, oid := range oids {
		if err := ValidateOID(oid); err != nil {
			return nil, err
		}
		if !seen[oid] {
			seen[oid] = true
			deduped = append(deduped, oid)
		}
	}
	if len(deduped) == 0 {
		return nil, nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	// #nosec G204 - r.path is controlled by us
	cmd := exec.CommandContext(ctx, "git", "-C", r.path, "cat-file", "--batch-check", "--no-buffer")
	cmd.Stdin = strings.NewReader(strings.Join(deduped, "\n") + "\n")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, errors.Wrapf(err, "git cat-file --batch-check: %s", stderr.String())
	}

	var missing []string
	for line := range strings.SplitSeq(strings.TrimRight(stdout.String(), "\n"), "\n") {
		if strings.HasSuffix(line, " missing") {
			// Output format is "<oid> missing"; extract the oid.
			oid, _, _ := strings.Cut(line, " ")
			missing = append(missing, oid)
		}
	}
	return missing, nil
}

// ValidateOID returns an error if oid is not a 40- or 64-character lower-case hex string.
func ValidateOID(oid string) error {
	if len(oid) != 40 && len(oid) != 64 {
		return errors.Errorf("invalid object ID %q: must be 40 or 64 hex characters", oid)
	}
	for _, c := range oid {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return errors.Errorf("invalid object ID %q: must be lower-case hex", oid)
		}
	}
	return nil
}
