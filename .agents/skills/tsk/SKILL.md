---
name: tsk
description: Enable, configure, and write tsk Go linters. Use when the user asks to enforce a convention or architectural constraint, to fix a kind of issue across the codebase, or to change .tsk/config.toml or a .tsk/*.ts linter.
---

# tsk

`tsk` runs Go linters written in TypeScript. The repository's config is
`.tsk/config.toml`, and its own linters are `.tsk/<name>.ts`. The full
reference is the README at https://github.com/alecthomas/tsk.

## Commands

- `tsk [packages...]` lints `./...`, or the given packages. `--fix` applies
  suggested fixes. `--enable` and `--disable` override the config for one run.
- `tsk list` lists every linter, whether it is enabled, and its summary.
  `tsk list --json` adds each linter's full documentation.
- `tsk config <linter>` prints a linter's options, documented and set to their
  defaults.
- `tsk check` type-checks the scripts and validates the config.
- `tsk test [--enable <linter>]` runs linters against their test data.
- `tsk editor` writes TypeScript declarations to `.tsk/types/` for editors.

## Enable an existing linter

1. Find candidates with `tsk list --json`, and read their documentation.
2. Measure the impact with `tsk --enable <linter>` before changing the config.
3. Add the linter to `enable` in `.tsk/config.toml`. If the config does not
   set `disable-all`, a linter on by default needs only removing from
   `disable`.
4. Set options in a table named after the linter, using `tsk config <linter>`
   for the names and defaults. Option names are kebab-case.
5. Fix the existing findings, or keep intentional ones with
   `//nolint:<linter> // <reason>`. Use `--fix` where the linter offers fixes.

Narrow a linter with an `[[exclude]]` rule rather than disabling it. A rule
names `linters` and any of `path`, `path-except`, `text`, and `source`, each a
regular expression. `skip-tests` drops a linter's findings in `_test.go` files.

## Write a linter

Write one when no existing linter or configuration enforces the rule.

Write a linter for the kind of rule, not for one instance of it. Make the
specifics of the rule options, and set the requested rule in
`.tsk/config.toml`. For example, a rule against `env:"..."` struct tags other
than `env:"-"` belongs in a general struct tag linter, with options naming the
tags and values to allow or deny. If an existing `.tsk/` linter covers the same
kind of rule, add an option to it instead of writing another. Generalise only
what varies between rules of that kind; do not add options nothing needs.

1. Put the linter in `.tsk/<name>.ts`. It default-exports
   `defineAnalyzer({ name, doc, requires, run(pass) })` from `"tsk"`, named
   as the file is. `run` mirrors a Go `analysis.Analyzer`:
   import Go packages such as `"go/ast"` and `"go/types"` by path, with
   lowerCamelCase names, and report with
   `pass.report({ pos, message })`.
2. Traverse syntax with the `inspect` pass from `"tsk/passes"`, as in
   `pass.resultOf(inspect).root().preorder(ast.CallExpr)`, so Go filters the
   nodes.
3. Add test data under `.tsk/testdata/<name>/`, as a module or a GOPATH tree
   under `src/`. Mark each expected finding with a ``// want `regexp` ``
   comment, and include code the linter must accept. Test the options with
   cases in `.tsk/testdata/<name>/tsk.test.toml`, each setting `config`.
4. Run `tsk check`, then `tsk test --enable <name>`, until both pass.
5. Run `tsk` and fix existing violations in the same change, or keep
   intentional ones with `//nolint:<name> // <reason>`.
6. Note in the repository's agent instructions that the linter enforces the
   rule.

Keep a linter precise: a false positive costs more than a missed finding.
Report a message that says what to do instead. Offer a suggested fix only when
it is unambiguous. Run `tsk editor` for declarations of the full script API,
which the README describes under "Writing linters".
