package main

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/alecthomas/errors"

	"github.com/block/cachew/gitcredential"
	"github.com/block/cachew/internal/azuregitcredential"
)

type commandLine struct {
	Audience       string `name:"audience" help:"Microsoft Entra access-token audience; /.default is appended when absent." required:""`
	CredentialMode string `name:"credential" help:"Azure Identity credential type." enum:"workload-identity,default,managed-identity" default:"workload-identity"`
}

func main() {
	gitcredential.CommandMain(&commandLine{}, func(_ context.Context, cli *commandLine) (gitcredential.CommandHandler, error) {
		credential, err := newAzureCredential(cli.CredentialMode)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		helper, err := azuregitcredential.New(credential, cli.Audience)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return helper, nil
	})
}

func newAzureCredential(mode string) (azcore.TokenCredential, error) {
	switch mode {
	case "workload-identity":
		credential, err := azidentity.NewWorkloadIdentityCredential(nil)
		return credential, errors.WithStack(err)
	case "default":
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		return credential, errors.WithStack(err)
	case "managed-identity":
		credential, err := azidentity.NewManagedIdentityCredential(nil)
		return credential, errors.WithStack(err)
	default:
		return nil, errors.Errorf("unsupported credential mode %q", mode)
	}
}
