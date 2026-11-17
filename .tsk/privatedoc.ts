import * as ast from "go/ast";
import { defineAnalyzer } from "tsk";
import { inspect } from "tsk/passes";

interface Config {
  /** An unexported function shorter than this many lines may not have a doc comment. */
  minLines: number;
}

export default defineAnalyzer<Config>({
  name: "privatedoc",
  doc: `report doc comments on short unexported functions

A short unexported function should be clear from its name and body. Comments
are for exported declarations and for long, non-obvious code.`,
  requires: [inspect],
  config: { minLines: 20 },
  run(pass) {
    for (const cursor of pass.resultOf(inspect).root().preorder(ast.FuncDecl)) {
      const decl = cursor.node() as ast.FuncDecl;
      if (decl.doc === null || decl.name === null || ast.isExported(decl.name.name)) {
        continue;
      }
      // Directives such as //go:noinline sit in the doc group but are not prose.
      if (decl.doc.text().trim() === "") {
        continue;
      }
      const lines = pass.fset.position(decl.end()).line - pass.fset.position(decl.pos()).line + 1;
      if (lines >= pass.config.minLines) {
        continue;
      }
      pass.report({
        pos: decl.pos(),
        end: decl.name.end(),
        message: `remove the doc comment on ${decl.name.name}: unexported functions under ${pass.config.minLines} lines should be clear from their name and body`,
      });
    }
  },
});
