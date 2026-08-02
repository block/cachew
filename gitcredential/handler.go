package gitcredential

import (
	"context"
	"io"
	"time"

	"github.com/alecthomas/errors"
)

// CommandResult is the authorization returned by an external provider's core logic.
type CommandResult struct {
	Authorization string
	ExpiresAt     time.Time
}

// CommandHandler obtains an authorization value for a canonical Git remote URL.
type CommandHandler interface {
	Credential(ctx context.Context, remoteURL string) (CommandResult, error)
}

// CommandHandlerFunc adapts a function to CommandHandler.
type CommandHandlerFunc func(context.Context, string) (CommandResult, error)

// Credential calls f.
func (f CommandHandlerFunc) Credential(ctx context.Context, remoteURL string) (CommandResult, error) {
	return f(ctx, remoteURL)
}

// ServeCommand decodes one request, calls handler, and writes one response.
func ServeCommand(ctx context.Context, stdin io.Reader, stdout io.Writer, handler CommandHandler) error {
	if handler == nil {
		return errors.New("credential command handler is required")
	}
	request, err := DecodeRequest(stdin)
	if err != nil {
		return errors.WithStack(err)
	}
	result, err := handler.Credential(ctx, request.RemoteURL)
	if err != nil {
		return errors.WithStack(err)
	}
	if err := EncodeResponse(stdout, Response{
		Version:       ProtocolVersion,
		Authorization: result.Authorization,
		ExpiresAt:     result.ExpiresAt,
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
