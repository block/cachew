package gitcredential

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/alecthomas/errors"
	"github.com/alecthomas/kong"
)

// CommandHandlerFactory constructs a handler after Kong has populated provider options.
type CommandHandlerFactory[T any] func(context.Context, *T) (CommandHandler, error)

// RunCommandCLI parses provider options with Kong and serves one credential request.
func RunCommandCLI[T any](
	ctx context.Context,
	args []string,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	options *T,
	factory CommandHandlerFactory[T],
	kongOptions ...kong.Option,
) error {
	if options == nil {
		return errors.New("CLI options are required")
	}
	if factory == nil {
		return errors.New("credential command handler factory is required")
	}
	parserOptions := append([]kong.Option{}, kongOptions...)
	parserOptions = append(parserOptions,
		kong.Writers(stdout, stderr),
		kong.UsageOnError(),
		kong.HelpOptions{Compact: true},
		kong.Exit(func(code int) { panic(commandCLIExit(code)) }),
	)
	parser, err := kong.New(options, parserOptions...)
	if err != nil {
		return errors.Wrap(err, "create credential command CLI")
	}
	proceed, err := parseCommandCLI(parser, args)
	if err != nil {
		return errors.Wrap(err, "parse credential command CLI")
	}
	if !proceed {
		return nil
	}
	handler, err := factory(ctx, options)
	if err != nil {
		return errors.Wrap(err, "create credential command handler")
	}
	return ServeCommand(ctx, stdin, stdout, handler)
}

type commandCLIExit int

func parseCommandCLI(parser *kong.Kong, args []string) (proceed bool, returnErr error) {
	proceed = true
	defer func() {
		if recovered := recover(); recovered != nil {
			code, ok := recovered.(commandCLIExit)
			if !ok {
				panic(recovered)
			}
			proceed = false
			if code != 0 {
				returnErr = errors.Errorf("CLI exited with status %d", code)
			}
		}
	}()
	_, err := parser.Parse(args)
	if err != nil {
		return proceed, errors.WithStack(err)
	}
	return proceed, nil
}

// CommandMain runs a Kong-based external credential command using the process arguments and standard streams.
func CommandMain[T any](options *T, factory CommandHandlerFactory[T], kongOptions ...kong.Option) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	err := RunCommandCLI(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, options, factory, kongOptions...)
	cancel()
	if err == nil {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "%s: %v\n", filepath.Base(os.Args[0]), err) //nolint:forbidigo,gosec // CLI diagnostics require the returned provider error.
	os.Exit(1)
}
