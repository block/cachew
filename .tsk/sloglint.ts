import * as ast from "go/ast";
import type { Cursor } from "golang.org/x/tools/go/ast/inspector";
import * as typeutil from "golang.org/x/tools/go/types/typeutil";
import { defineAnalyzer, type Pass } from "tsk";
import { inspect } from "tsk/passes";

interface Config {
  /** Report global loggers: "all", "default" (the default logger only), or "" (off). */
  noGlobal: string;
  /** Report calls without a context: "all", "scope" (where one is in scope), or "" (off). */
  context: string;
}

const globalChecked = new Set([
  "Log",
  "LogAttrs",
  "Debug",
  "Info",
  "Warn",
  "Error",
  "DebugContext",
  "InfoContext",
  "WarnContext",
  "ErrorContext",
  "With",
]);

const contextless = new Set(["Debug", "Info", "Warn", "Error"]);

export default defineAnalyzer<Config>({
  name: "sloglint",
  doc: `check log/slog calls for global loggers and missing contexts

Ports the no-global and context options of go-simpler/sloglint. With no-global,
calls through the default logger, as slog.Info, and through package-level
*slog.Logger variables are reported. With context, Debug, Info, Warn, and Error
are reported where their Context variants should be used, with a fix when the
enclosing function has a context.Context or *http.Request parameter.`,
  requires: [inspect],
  config: { noGlobal: "", context: "" },
  run(pass) {
    for (const cursor of pass.resultOf(inspect).root().preorder(ast.CallExpr)) {
      const call = cursor.node() as ast.CallExpr;
      const name = slogFunctionName(pass, call);
      if (name === null || call.fun?.$type !== "SelectorExpr") {
        continue;
      }
      if (pass.config.noGlobal !== "" && globalChecked.has(name)) {
        checkGlobal(pass, call.fun, pass.config.noGlobal === "default");
      }
      if (pass.config.context !== "" && contextless.has(name)) {
        checkContext(pass, cursor, call, call.fun, name, pass.config.context === "scope");
      }
    }
  },
});

// slogFunctionName returns the name of the log/slog function or *slog.Logger
// method that call invokes, or null for any other call.
function slogFunctionName(pass: Pass<Config>, call: ast.CallExpr): string | null {
  const fullName = typeutil.staticCallee(pass.typesInfo, call)?.fullName() ?? "";
  const match = /^(?:log\/slog\.|\(\*log\/slog\.Logger\)\.)(\w+)$/.exec(fullName);
  return match === null ? null : match[1];
}

function checkGlobal(pass: Pass<Config>, sel: ast.SelectorExpr, defaultOnly: boolean): void {
  if (sel.x?.$type !== "Ident") {
    return;
  }
  const object = pass.typesInfo.objectOf(sel.x);
  if (object?.$type === "PkgName") {
    pass.report({ pos: sel.x.pos(), end: sel.x.end(), message: "default logger should not be used" });
    return;
  }
  if (!defaultOnly && object !== null && object.parent() === object.pkg()?.scope()) {
    pass.report({ pos: sel.x.pos(), end: sel.x.end(), message: "global logger should not be used" });
  }
}

function checkContext(
  pass: Pass<Config>,
  cursor: Cursor,
  call: ast.CallExpr,
  sel: ast.SelectorExpr,
  name: string,
  scopeOnly: boolean,
): void {
  const message = `${name}Context should be used instead`;
  const method = sel.sel!;
  if (!scopeOnly) {
    pass.report({ pos: method.pos(), end: method.end(), message });
    return;
  }
  const context = contextInScope(pass, cursor);
  if (context === null) {
    return;
  }
  pass.report({
    pos: method.pos(),
    end: method.end(),
    message,
    suggestedFixes: [
      {
        message: `Use ${name}Context`,
        textEdits: [{ pos: method.pos(), end: call.lparen + 1, newText: `${name}Context(${context}, ` }],
      },
    ],
  });
}

// contextInScope returns an expression for the context of the nearest
// enclosing function with a context.Context or *http.Request parameter.
function contextInScope(pass: Pass<Config>, cursor: Cursor): string | null {
  for (const enclosing of cursor.enclosing(ast.FuncDecl, ast.FuncLit)) {
    const fn = enclosing.node() as ast.FuncDecl | ast.FuncLit;
    for (const field of fn.type!.params?.list ?? []) {
      const param = field?.names[0];
      if (param === undefined || param === null) {
        continue;
      }
      switch (pass.typesInfo.typeOf(param)?.string()) {
        case "context.Context":
          return param.name;
        case "*net/http.Request":
          return `${param.name}.Context()`;
      }
    }
  }
  return null;
}
