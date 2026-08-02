package git

import (
	"context"
	"net/http"
	"strings"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
	"github.com/block/cachew/internal/logging"
)

func (s *Strategy) forwardToUpstream(w http.ResponseWriter, r *http.Request, host, pathValue string) {
	logger := logging.FromContext(r.Context())

	logger.DebugContext(r.Context(), "Forwarding to upstream", "method", r.Method, "host", host, "path", pathValue)

	repositoryURL, ok := upstreamRepositoryURL(host, pathValue)
	if ok {
		credential, matched, err := s.cloneManager.Credential(r.Context(), repositoryURL)
		if err != nil {
			logger.ErrorContext(r.Context(), "Failed to obtain upstream Git credential", "error", err)
			http.Error(w, "Upstream credential unavailable", http.StatusBadGateway)
			return
		}
		if matched {
			if err := applyUpstreamCredential(r, repositoryURL, credential); err != nil {
				logger.ErrorContext(r.Context(), "Invalid upstream Git credential", "error", err)
				http.Error(w, "Upstream credential unavailable", http.StatusBadGateway)
				return
			}
			r = r.WithContext(context.WithValue(r.Context(), upstreamCredentialAppliedKey{}, true))
		}
	}

	s.proxy.ServeHTTP(w, r)
}

type upstreamCredentialAppliedKey struct{}

func upstreamCredentialApplied(ctx context.Context) bool {
	applied, _ := ctx.Value(upstreamCredentialAppliedKey{}).(bool)
	return applied
}

func upstreamRepositoryURL(host, pathValue string) (string, bool) {
	repositoryPath := strings.TrimPrefix(pathValue, "/")
	for _, suffix := range []string{"/info/refs", "/git-upload-pack", "/git-receive-pack"} {
		if trimmed, found := strings.CutSuffix(repositoryPath, suffix); found {
			return "https://" + host + "/" + trimmed, trimmed != ""
		}
	}
	if index := strings.LastIndex(repositoryPath, "/info/lfs/"); index >= 0 {
		repositoryPath = repositoryPath[:index]
		return "https://" + host + "/" + repositoryPath, repositoryPath != ""
	}
	if trimmed, found := strings.CutSuffix(repositoryPath, "/info/lfs"); found {
		return "https://" + host + "/" + trimmed, trimmed != ""
	}
	return "", false
}

func applyUpstreamCredential(r *http.Request, repositoryURL string, credential gitcredential.Credential) error {
	if credential.Authorization == "" || strings.TrimSpace(credential.Authorization) != credential.Authorization ||
		strings.ContainsAny(credential.Authorization, "\r\n\x00") {
		return errors.New("credential provider returned an invalid authorization value")
	}
	expectedScope, err := gitcredential.NormalizeRepositoryURLScope(repositoryURL)
	if err != nil {
		return errors.Wrap(err, "normalize credential URL scope")
	}
	if credential.URLScope != expectedScope {
		return errors.New("credential provider returned an invalid URL scope")
	}
	r.Header.Set("Authorization", credential.Authorization)
	return nil
}
