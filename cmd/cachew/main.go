package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"

	"github.com/block/cachew/client"
	"github.com/block/cachew/internal/logging"
)

type CLI struct {
	LoggingConfig logging.Config `embed:"" prefix:"log-"`

	URL           string `help:"Remote cache server URL." default:"http://127.0.0.1:8080"`
	Authorization string `help:"Authorization header value (e.g. 'Bearer <token>')."`
	Platform      bool   `help:"Prefix keys with platform ($${os}-$${arch}-)."`
	Daily         bool   `help:"Prefix keys with date ($${YYYY}-$${MM}-$${DD}-). Mutually exclusive with --hourly." xor:"timeprefix"`
	Hourly        bool   `help:"Prefix keys with date and hour ($${YYYY}-$${MM}-$${DD}-$${HH}-). Mutually exclusive with --daily." xor:"timeprefix"`

	Get        GetCmd        `cmd:"" help:"Download object from cache." group:"Operations:"`
	Stat       StatCmd       `cmd:"" help:"Show metadata for cached object." group:"Operations:"`
	Put        PutCmd        `cmd:"" help:"Upload object to cache." group:"Operations:"`
	Delete     DeleteCmd     `cmd:"" help:"Remove object from cache." group:"Operations:"`
	Namespaces NamespacesCmd `cmd:"" help:"List available namespaces in cache." group:"Operations:"`

	Save    SaveCmd    `cmd:"" help:"Create compressed archive of directory and upload." group:"Snapshots:"`
	Restore RestoreCmd `cmd:"" help:"Download and extract archive to directory." group:"Snapshots:"`

	Git GitCmd `cmd:"" help:"Git-aware operations." group:"Git:"`
}

func main() { os.Exit(run()) }

func run() int {
	cli := CLI{}
	kctx := kong.Parse(&cli, kong.UsageOnError(), kong.HelpOptions{Compact: true}, kong.DefaultEnvars("CACHEW"), kong.Bind(&cli))
	ctx := context.Background()
	_, ctx = logging.Configure(ctx, cli.LoggingConfig)

	var headerFunc client.HeaderFunc
	if cli.Authorization != "" {
		headerFunc = func() http.Header {
			return http.Header{"Authorization": {cli.Authorization}}
		}
	}
	c := client.New(cli.URL, headerFunc)
	defer c.Close()

	kctx.BindTo(ctx, (*context.Context)(nil))
	kctx.Bind(c)
	kctx.Bind(c.HTTP())
	err := kctx.Run(ctx)
	if errors.Is(err, errCacheMiss) {
		return 2
	}
	kctx.FatalIfErrorf(err)
	return 0
}

// errCacheMiss signals that a restore found no object for the requested key.
// Sentinel so main can exit with code 2 (distinct from generic errors at
// code 1), matching conventions used by grep/diff.
var errCacheMiss = errors.New("cache miss")

type GetCmd struct {
	Namespace client.Namespace `arg:"" help:"Namespace for organizing cache objects."`
	Key       PlatformKey      `arg:"" help:"Object key (hex or string)."`
	Output    *os.File         `short:"o" help:"Output file (default: stdout)." default:"-"`
}

func (c *GetCmd) Run(ctx context.Context, api *client.Client) error {
	defer c.Output.Close()

	rc, headers, err := api.Namespace(c.Namespace).Open(ctx, c.Key.Key())
	if err != nil {
		return errors.Wrap(err, "failed to open object")
	}
	defer rc.Close()

	for key, values := range headers {
		for _, value := range values {
			fmt.Fprintf(os.Stderr, "%s: %s\n", key, value) //nolint:forbidigo
		}
	}

	_, err = io.Copy(c.Output, rc)
	return errors.Wrap(err, "failed to copy data")
}

type StatCmd struct {
	Namespace client.Namespace `arg:"" help:"Namespace for organizing cache objects."`
	Key       PlatformKey      `arg:"" help:"Object key (hex or string)."`
}

func (c *StatCmd) Run(ctx context.Context, api *client.Client) error {
	headers, err := api.Namespace(c.Namespace).Stat(ctx, c.Key.Key())
	if err != nil {
		return errors.Wrap(err, "failed to stat object")
	}

	for key, values := range headers {
		for _, value := range values {
			fmt.Printf("%s: %s\n", key, value) //nolint:forbidigo
		}
	}

	return nil
}

type PutCmd struct {
	Namespace client.Namespace  `arg:"" help:"Namespace for organizing cache objects."`
	Key       PlatformKey       `arg:"" help:"Object key (hex or string)."`
	Input     *os.File          `arg:"" help:"Input file (default: stdin)." default:"-"`
	TTL       time.Duration     `help:"Time to live for the object."`
	Headers   map[string]string `short:"H" help:"Additional headers (key=value)."`
}

func (c *PutCmd) Run(ctx context.Context, api *client.Client) error {
	defer c.Input.Close()

	headers := make(http.Header)
	for key, value := range c.Headers {
		headers.Set(key, value)
	}

	if filename := getFilename(c.Input); filename != "" {
		headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(filename))) //nolint:perfsprint
	}

	createCtx, cancelCreate := context.WithCancelCause(ctx)
	defer cancelCreate(nil)

	wc, err := api.Namespace(c.Namespace).Create(createCtx, c.Key.Key(), headers, c.TTL)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	if _, err := io.Copy(wc, c.Input); err != nil {
		cancelCreate(err)
		return errors.Join(errors.Wrap(err, "failed to copy data"), wc.Close())
	}

	return errors.Wrap(wc.Close(), "failed to close writer")
}

type DeleteCmd struct {
	Namespace client.Namespace `arg:"" help:"Namespace for organizing cache objects."`
	Key       PlatformKey      `arg:"" help:"Object key (hex or string)."`
}

func (c *DeleteCmd) Run(ctx context.Context, api *client.Client) error {
	return errors.Wrap(api.Namespace(c.Namespace).Delete(ctx, c.Key.Key()), "failed to delete object")
}

type NamespacesCmd struct{}

func (c *NamespacesCmd) Run(ctx context.Context, api *client.Client) error {
	namespaces, err := api.ListNamespaces(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to list namespaces")
	}

	if len(namespaces) == 0 {
		fmt.Println("No namespaces found") //nolint:forbidigo
		return nil
	}

	for _, ns := range namespaces {
		fmt.Println(ns) //nolint:forbidigo
	}
	return nil
}

type SaveCmd struct {
	Namespace client.Namespace `arg:"" help:"Namespace for organizing cache objects."`
	Directory string           `arg:"" help:"Directory containing paths to archive." type:"path"`
	Paths     []string         `arg:"" optional:"" help:"Paths within Directory to archive (default: \".\")."`

	Key       string   `help:"Object key (hex or string)." xor:"cache-key" required:""`
	HashFiles []string `short:"H" help:"Compute key from SHA256 of files matched by doublestar glob patterns (repeatable)." xor:"cache-key" required:""`

	TTL         time.Duration `help:"Time to live for the object."`
	Exclude     []string      `help:"Patterns to exclude (tar --exclude syntax)."`
	ZstdThreads int           `help:"Threads for zstd compression (0 = all CPU cores)." default:"0"`
}

func (c *SaveCmd) Run(ctx context.Context, api *client.Client, cli *CLI) error {
	key, display, err := resolveKey(cli, c.Key, c.HashFiles)
	if err != nil {
		return err
	}
	paths := c.Paths
	if len(paths) == 0 {
		paths = []string{"."}
	}
	fmt.Fprintf(os.Stderr, "Archiving %s...\n", c.Directory) //nolint:forbidigo
	err = api.Namespace(c.Namespace).Save(ctx, key, c.Directory, paths,
		client.WithTTL(c.TTL),
		client.WithExclude(c.Exclude...),
		client.WithZstdThreads(c.ZstdThreads),
	)
	if err != nil {
		return errors.Wrap(err, "failed to save")
	}

	fmt.Fprintf(os.Stderr, "Saved: %s\n", display) //nolint:forbidigo
	return nil
}

type RestoreCmd struct {
	Namespace client.Namespace `arg:"" help:"Namespace for organizing cache objects."`
	Directory string           `arg:"" help:"Target directory for extraction." type:"path"`

	Key       string   `help:"Object key (hex or string)." xor:"cache-key" required:""`
	HashFiles []string `short:"H" help:"Compute key from SHA256 of files matched by doublestar glob patterns (repeatable)." xor:"cache-key" required:""`

	ZstdThreads int `help:"Threads for zstd decompression (0 = all CPU cores)." default:"0"`
}

func (c *RestoreCmd) Run(ctx context.Context, api *client.Client, cli *CLI) error {
	key, display, err := resolveKey(cli, c.Key, c.HashFiles)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "Restoring to %s...\n", c.Directory) //nolint:forbidigo
	hit, err := api.Namespace(c.Namespace).Restore(ctx, key, c.Directory,
		client.WithZstdThreads(c.ZstdThreads))
	if err != nil {
		return errors.Wrap(err, "failed to restore")
	}
	if !hit {
		fmt.Fprintf(os.Stderr, "Cache miss: %s\n", display) //nolint:forbidigo
		return errCacheMiss
	}

	fmt.Fprintf(os.Stderr, "Restored: %s\n", display) //nolint:forbidigo
	return nil
}

// resolveKey returns the final Key and a human-readable form for logging,
// using exactly one of key or hashFiles (enforced by kong's xor group), then
// applying any global --platform / --daily / --hourly prefixes.
func resolveKey(cli *CLI, key string, hashFiles []string) (client.Key, string, error) {
	raw := key
	if len(hashFiles) > 0 {
		k, err := client.HashFiles(hashFiles...)
		if err != nil {
			return client.Key{}, "", errors.Wrap(err, "failed to hash files")
		}
		raw = k.String()
	}
	prefixed := applyKeyPrefixes(cli, raw)
	var final client.Key
	if err := final.UnmarshalText([]byte(prefixed)); err != nil {
		return client.Key{}, "", errors.WithStack(err)
	}
	return final, prefixed, nil
}

func applyKeyPrefixes(cli *CLI, raw string) string {
	prefixed := raw
	if cli.Platform {
		prefixed = fmt.Sprintf("%s-%s-%s", runtime.GOOS, runtime.GOARCH, prefixed)
	}
	now := time.Now()
	switch {
	case cli.Hourly:
		prefixed = now.Format("2006-01-02-15-") + prefixed
	case cli.Daily:
		prefixed = now.Format("2006-01-02-") + prefixed
	}
	return prefixed
}

func getFilename(f *os.File) string {
	info, err := f.Stat()
	if err != nil {
		return ""
	}

	if !info.Mode().IsRegular() {
		return ""
	}

	return f.Name()
}

// PlatformKey wraps a client.Key and stores the original input for platform prefixing.
type PlatformKey struct {
	raw string
	key client.Key
}

func (pk *PlatformKey) UnmarshalText(text []byte) error {
	pk.raw = string(text)
	return errors.WithStack(pk.key.UnmarshalText(text))
}

func (pk *PlatformKey) Key() client.Key {
	return pk.key
}

func (pk *PlatformKey) String() string {
	return pk.key.String()
}

func (pk *PlatformKey) AfterApply(cli *CLI) error {
	return errors.WithStack(pk.key.UnmarshalText([]byte(applyKeyPrefixes(cli, pk.raw))))
}
