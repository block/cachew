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

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/snapshot"
)

type CLI struct {
	LoggingConfig logging.Config `embed:"" prefix:"log-"`

	URL      string `help:"Remote cache server URL." default:"http://127.0.0.1:8080"`
	Platform bool   `help:"Prefix keys with platform ($${os}-$${arch}-)."`

	Get    GetCmd    `cmd:"" help:"Download object from cache." group:"Operations:"`
	Stat   StatCmd   `cmd:"" help:"Show metadata for cached object." group:"Operations:"`
	Put    PutCmd    `cmd:"" help:"Upload object to cache." group:"Operations:"`
	Delete DeleteCmd `cmd:"" help:"Remove object from cache." group:"Operations:"`

	Snapshot SnapshotCmd `cmd:"" help:"Create compressed archive of directory and upload." group:"Snapshots:"`
	Restore  RestoreCmd  `cmd:"" help:"Download and extract archive to directory." group:"Snapshots:"`
}

func main() {
	cli := CLI{}
	kctx := kong.Parse(&cli, kong.UsageOnError(), kong.HelpOptions{Compact: true}, kong.DefaultEnvars("CACHEW"), kong.Bind(&cli))
	ctx := context.Background()
	_, ctx = logging.Configure(ctx, cli.LoggingConfig)

	remote := cache.NewRemote(cli.URL)
	defer remote.Close()

	kctx.BindTo(ctx, (*context.Context)(nil))
	kctx.BindTo(remote, (*cache.Cache)(nil))
	kctx.FatalIfErrorf(kctx.Run(ctx))
}

type GetCmd struct {
	Key    PlatformKey `arg:"" help:"Object key (hex or string)."`
	Output *os.File    `short:"o" help:"Output file (default: stdout)." default:"-"`
}

func (c *GetCmd) Run(ctx context.Context, cache cache.Cache) error {
	defer c.Output.Close()

	rc, headers, err := cache.Open(ctx, c.Key.Key())
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
	Key PlatformKey `arg:"" help:"Object key (hex or string)."`
}

func (c *StatCmd) Run(ctx context.Context, cache cache.Cache) error {
	headers, err := cache.Stat(ctx, c.Key.Key())
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
	Key     PlatformKey       `arg:"" help:"Object key (hex or string)."`
	Input   *os.File          `arg:"" help:"Input file (default: stdin)." default:"-"`
	TTL     time.Duration     `help:"Time to live for the object."`
	Headers map[string]string `short:"H" help:"Additional headers (key=value)."`
}

func (c *PutCmd) Run(ctx context.Context, cache cache.Cache) error {
	defer c.Input.Close()

	headers := make(http.Header)
	for key, value := range c.Headers {
		headers.Set(key, value)
	}

	if filename := getFilename(c.Input); filename != "" {
		headers.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(filename))) //nolint:perfsprint
	}

	wc, err := cache.Create(ctx, c.Key.Key(), headers, c.TTL)
	if err != nil {
		return errors.Wrap(err, "failed to create object")
	}

	if _, err := io.Copy(wc, c.Input); err != nil {
		return errors.Join(errors.Wrap(err, "failed to copy data"), wc.Close())
	}

	return errors.Wrap(wc.Close(), "failed to close writer")
}

type DeleteCmd struct {
	Key PlatformKey `arg:"" help:"Object key (hex or string)."`
}

func (c *DeleteCmd) Run(ctx context.Context, cache cache.Cache) error {
	return errors.Wrap(cache.Delete(ctx, c.Key.Key()), "failed to delete object")
}

type SnapshotCmd struct {
	Key       PlatformKey   `arg:"" help:"Object key (hex or string)."`
	Directory string        `arg:"" help:"Directory to archive." type:"path"`
	TTL       time.Duration `help:"Time to live for the object."`
	Exclude   []string      `help:"Patterns to exclude (tar --exclude syntax)."`
}

func (c *SnapshotCmd) Run(ctx context.Context, cache cache.Cache) error {
	fmt.Fprintf(os.Stderr, "Archiving %s...\n", c.Directory) //nolint:forbidigo
	if err := snapshot.Create(ctx, cache, c.Key.Key(), c.Directory, c.TTL, c.Exclude); err != nil {
		return errors.Wrap(err, "failed to create snapshot")
	}

	fmt.Fprintf(os.Stderr, "Snapshot uploaded: %s\n", c.Key.String()) //nolint:forbidigo
	return nil
}

type RestoreCmd struct {
	Key       PlatformKey `arg:"" help:"Object key (hex or string)."`
	Directory string      `arg:"" help:"Target directory for extraction." type:"path"`
}

func (c *RestoreCmd) Run(ctx context.Context, cache cache.Cache) error {
	fmt.Fprintf(os.Stderr, "Restoring to %s...\n", c.Directory) //nolint:forbidigo
	if err := snapshot.Restore(ctx, cache, c.Key.Key(), c.Directory); err != nil {
		return errors.Wrap(err, "failed to restore snapshot")
	}

	fmt.Fprintf(os.Stderr, "Snapshot restored: %s\n", c.Key.String()) //nolint:forbidigo
	return nil
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

// PlatformKey wraps a cache.Key and stores the original input for platform prefixing.
type PlatformKey struct {
	raw string
	key cache.Key
}

func (pk *PlatformKey) UnmarshalText(text []byte) error {
	pk.raw = string(text)
	return errors.WithStack(pk.key.UnmarshalText(text))
}

func (pk *PlatformKey) Key() cache.Key {
	return pk.key
}

func (pk *PlatformKey) String() string {
	return pk.key.String()
}

func (pk *PlatformKey) AfterApply(cli *CLI) error {
	if !cli.Platform {
		return nil
	}
	prefixed := fmt.Sprintf("%s-%s-%s", runtime.GOOS, runtime.GOARCH, pk.raw)
	return errors.WithStack(pk.key.UnmarshalText([]byte(prefixed)))
}
