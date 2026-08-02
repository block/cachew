package gitcredential

import (
	"net/url"
	"path"
	"strings"

	"github.com/alecthomas/errors"
)

// NormalizeRepositoryURL validates and canonicalizes an HTTPS repository URL for exact repository matching.
func NormalizeRepositoryURL(repositoryURL string) (string, error) {
	return normalizeRepositoryURL(repositoryURL, true)
}

// NormalizeRepositoryURLScope validates and normalizes an HTTPS repository URL without removing its Git path suffix.
func NormalizeRepositoryURLScope(repositoryURL string) (string, error) {
	return normalizeRepositoryURL(repositoryURL, false)
}

func normalizeRepositoryURL(repositoryURL string, matching bool) (string, error) {
	u, err := url.Parse(repositoryURL)
	if err != nil {
		return "", errors.Wrap(err, "parse repository URL")
	}
	if !strings.EqualFold(u.Scheme, "https") || u.Host == "" || u.Path == "" || !u.IsAbs() {
		return "", errors.New("repository URL must be an absolute HTTPS URL with a path")
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", errors.New("repository URL must not contain user information, a query, or a fragment")
	}
	if strings.Contains(u.Path, "\\") {
		return "", errors.New("repository URL path must not contain backslashes")
	}
	escapedPath := strings.ToLower(u.EscapedPath())
	if strings.Contains(escapedPath, "%2f") || strings.Contains(escapedPath, "%5c") {
		return "", errors.New("repository URL path must not contain escaped separators")
	}
	for component := range strings.SplitSeq(u.Path, "/") {
		if component == "." || component == ".." {
			return "", errors.New("repository URL path must not contain traversal components")
		}
	}

	hostname := strings.ToLower(u.Hostname())
	if hostname == "" {
		return "", errors.New("repository URL hostname is required")
	}
	port := u.Port()
	if port != "" && port != "443" {
		return "", errors.New("repository URL must not use a non-default HTTPS port")
	}
	host := hostname
	if strings.Contains(hostname, ":") {
		host = "[" + hostname + "]"
	}

	normalizedPath := strings.TrimSuffix(u.Path, "/")
	if matching {
		normalizedPath = path.Clean("/" + strings.TrimPrefix(normalizedPath, "/"))
		normalizedPath = strings.TrimSuffix(normalizedPath, ".git")
	}
	if normalizedPath == "" || normalizedPath == "/" {
		return "", errors.New("repository URL path must identify a repository")
	}

	return (&url.URL{Scheme: "https", Host: host, Path: normalizedPath}).String(), nil
}
