package gitcredential

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"
)

const maxCommandOutput = 64 << 10

// CommandConfig configures an external credential command and its exact repository matches.
type CommandConfig struct {
	Name          string        `hcl:"name,label"`
	Command       []string      `hcl:"command" help:"Executable and arguments for the credential provider."`
	Remotes       []string      `hcl:"remotes" help:"Exact HTTPS repository URLs authorized by this provider."`
	Timeout       time.Duration `hcl:"timeout,optional" default:"5s" help:"Maximum credential command execution time."`
	RefreshBefore time.Duration `hcl:"refresh-before,optional" default:"5m" help:"How early to refresh credentials before expiration."`
}

type command struct {
	name          string
	argv          []string
	refreshBefore time.Duration
	timeout       time.Duration
	remotes       map[string]struct{}
}

type cachedCredential struct {
	authorization string
	expiresAt     time.Time
}

// CommandProvider invokes configured external commands and caches successful credentials in memory.
type CommandProvider struct {
	commands        []command
	mu              sync.RWMutex
	cache           map[string]cachedCredential
	refreshes       singleflight.Group
	now             func() time.Time
	commandTotal    metric.Int64Counter
	commandDuration metric.Float64Histogram
	cacheTotal      metric.Int64Counter
}

// NewCommandProvider validates configs and creates an external command provider.
func NewCommandProvider(configs []CommandConfig) (*CommandProvider, error) {
	meter := otel.Meter("github.com/block/cachew/gitcredential")
	commandTotal, err := meter.Int64Counter("cachew.git.credential_command_total")
	if err != nil {
		return nil, errors.Wrap(err, "create credential command counter")
	}
	commandDuration, err := meter.Float64Histogram("cachew.git.credential_command_duration_seconds", metric.WithUnit("s"))
	if err != nil {
		return nil, errors.Wrap(err, "create credential command duration histogram")
	}
	cacheTotal, err := meter.Int64Counter("cachew.git.credential_cache_total")
	if err != nil {
		return nil, errors.Wrap(err, "create credential cache counter")
	}
	provider := &CommandProvider{
		cache:           make(map[string]cachedCredential),
		now:             time.Now,
		commandTotal:    commandTotal,
		commandDuration: commandDuration,
		cacheTotal:      cacheTotal,
	}
	seenNames := make(map[string]struct{}, len(configs))
	seenRemotes := make(map[string]string)
	for _, cfg := range configs {
		if strings.TrimSpace(cfg.Name) == "" {
			return nil, errors.New("git-credential-command name must not be empty")
		}
		if _, exists := seenNames[cfg.Name]; exists {
			return nil, errors.Errorf("duplicate git-credential-command name %q", cfg.Name)
		}
		seenNames[cfg.Name] = struct{}{}
		if len(cfg.Command) == 0 || strings.TrimSpace(cfg.Command[0]) == "" {
			return nil, errors.Errorf("git-credential-command %q command must not be empty", cfg.Name)
		}
		executable, err := exec.LookPath(cfg.Command[0])
		if err != nil {
			return nil, errors.Wrapf(err, "git-credential-command %q executable", cfg.Name)
		}
		if len(cfg.Remotes) == 0 {
			return nil, errors.Errorf("git-credential-command %q remotes must not be empty", cfg.Name)
		}
		if cfg.Timeout <= 0 {
			return nil, errors.Errorf("git-credential-command %q timeout must be positive", cfg.Name)
		}
		if cfg.RefreshBefore < 0 {
			return nil, errors.Errorf("git-credential-command %q refresh-before must be non-negative", cfg.Name)
		}

		configured := command{
			name:          cfg.Name,
			argv:          append([]string{executable}, cfg.Command[1:]...),
			refreshBefore: cfg.RefreshBefore,
			timeout:       cfg.Timeout,
			remotes:       make(map[string]struct{}, len(cfg.Remotes)),
		}
		for _, remote := range cfg.Remotes {
			canonical, err := NormalizeRepositoryURL(remote)
			if err != nil {
				return nil, errors.Wrapf(err, "git-credential-command %q remote %q", cfg.Name, remote)
			}
			if previous, exists := seenRemotes[canonical]; exists {
				return nil, errors.Errorf("duplicate git credential remote %q in providers %q and %q", canonical, previous, cfg.Name)
			}
			seenRemotes[canonical] = cfg.Name
			configured.remotes[canonical] = struct{}{}
		}
		provider.commands = append(provider.commands, configured)
	}
	return provider, nil
}

// Credential returns a credential when repositoryURL exactly matches a configured remote.
func (p *CommandProvider) Credential(ctx context.Context, repositoryURL string) (Credential, bool, error) {
	canonical, err := NormalizeRepositoryURL(repositoryURL)
	if err != nil {
		return Credential{}, false, nil //nolint:nilerr // Invalid URLs cannot match configured canonical remotes.
	}
	var selected *command
	for i := range p.commands {
		if _, ok := p.commands[i].remotes[canonical]; ok {
			selected = &p.commands[i]
			break
		}
	}
	if selected == nil {
		return Credential{}, false, nil
	}
	urlScope, err := NormalizeRepositoryURLScope(repositoryURL)
	if err != nil {
		return Credential{}, true, errors.Wrap(err, "normalize credential URL scope")
	}
	repositoryKey := selected.name + "\x00" + canonical
	if cached, ok := p.cached(repositoryKey, selected.refreshBefore); ok {
		p.cacheTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("provider", selected.name), attribute.String("result", "hit")))
		return Credential{Authorization: cached.authorization, URLScope: urlScope}, true, nil
	}
	p.cacheTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("provider", selected.name), attribute.String("result", "miss")))
	value, err, _ := p.refreshes.Do(repositoryKey, func() (any, error) {
		if cached, ok := p.cached(repositoryKey, selected.refreshBefore); ok {
			return cached, nil
		}
		return p.invoke(ctx, *selected, canonical, repositoryKey)
	})
	if err != nil {
		return Credential{}, true, errors.WithStack(err)
	}
	cached := value.(cachedCredential)
	return Credential{Authorization: cached.authorization, URLScope: urlScope}, true, nil
}

func (p *CommandProvider) cached(repositoryKey string, refreshBefore time.Duration) (cachedCredential, bool) {
	p.mu.RLock()
	entry, ok := p.cache[repositoryKey]
	p.mu.RUnlock()
	if !ok || !p.now().Add(refreshBefore).Before(entry.expiresAt) {
		return cachedCredential{}, false
	}
	return entry, true
}

func (p *CommandProvider) invoke(ctx context.Context, selected command, canonical, repositoryKey string) (cachedCredential, error) {
	started := p.now()
	status := "error"
	defer func() {
		attrs := metric.WithAttributes(attribute.String("provider", selected.name), attribute.String("status", status))
		p.commandTotal.Add(ctx, 1, attrs)
		p.commandDuration.Record(ctx, p.now().Sub(started).Seconds(), attrs)
	}()
	commandCtx, cancel := context.WithTimeout(ctx, selected.timeout)
	defer cancel()

	request, err := json.Marshal(Request{Version: ProtocolVersion, RemoteURL: canonical})
	if err != nil {
		return cachedCredential{}, errors.Wrap(err, "encode credential command request")
	}
	request = append(request, '\n')

	stdout := &limitedBuffer{limit: maxCommandOutput}
	stderr := &limitedBuffer{limit: maxCommandOutput}
	cmd := exec.CommandContext(commandCtx, selected.argv[0], selected.argv[1:]...) // #nosec G204 -- executable is startup-validated configuration.
	cmd.WaitDelay = time.Second
	cmd.Stdin = bytes.NewReader(request)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		if commandCtx.Err() != nil {
			status = "timeout"
			return cachedCredential{}, errors.Wrapf(commandCtx.Err(), "credential provider %q timed out", selected.name)
		}
		return cachedCredential{}, errors.Wrapf(err, "credential provider %q failed", selected.name)
	}
	if stdout.exceeded {
		status = "invalid"
		return cachedCredential{}, errors.Errorf("credential provider %q response exceeds %d bytes", selected.name, maxCommandOutput)
	}

	response, err := parseCommandResponse(stdout.Bytes(), p.now())
	if err != nil {
		status = "invalid"
		return cachedCredential{}, errors.Wrapf(err, "credential provider %q returned an invalid response", selected.name)
	}

	cached := cachedCredential{authorization: response.Authorization, expiresAt: response.ExpiresAt}
	p.mu.Lock()
	p.cache[repositoryKey] = cached
	p.mu.Unlock()
	status = "success"
	return cached, nil
}

func parseCommandResponse(data []byte, now time.Time) (Response, error) {
	var response Response
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&response); err != nil {
		return Response{}, errors.Wrap(err, "decode JSON")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Response{}, errors.Wrap(err, "decode JSON")
	}
	if response.Version != ProtocolVersion {
		return Response{}, errors.Errorf("unsupported version %d", response.Version)
	}
	if response.Authorization == "" || strings.TrimSpace(response.Authorization) != response.Authorization ||
		strings.ContainsAny(response.Authorization, "\r\n\x00") {
		return Response{}, errors.New("invalid authorization value")
	}
	if response.ExpiresAt.IsZero() || !response.ExpiresAt.After(now) {
		return Response{}, errors.New("expired credential")
	}
	return response, nil
}

type limitedBuffer struct {
	bytes.Buffer
	limit    int
	exceeded bool
}

func (w *limitedBuffer) Write(data []byte) (int, error) {
	originalLength := len(data)
	remaining := w.limit - w.Len()
	if remaining < len(data) {
		w.exceeded = true
		if remaining > 0 {
			_, _ = w.Buffer.Write(data[:remaining])
		}
		return originalLength, nil
	}
	_, _ = w.Buffer.Write(data)
	return originalLength, nil
}
