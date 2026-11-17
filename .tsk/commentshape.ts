import * as ast from "go/ast";
import { defineAnalyzer } from "tsk";

interface Config {
  /** The most lines a comment's opening summary may span. */
  titleLines: number;
  /** The most lines of explanation that may follow the summary after a blank comment line. */
  bodyLines: number;
}

// Directives are //go:build and //nolint:x, plus gosec's "// #nosec", which breaks the no-space convention.
const directive = /^\/\/([a-z]+:\S|\s*#nosec\b)/;

export default defineAnalyzer<Config>({
  name: "commentshape",
  doc: `report comments longer than a summary and a short explanation

A comment is a summary, optionally followed by a blank comment line and a
short explanation. Longer comments usually restate the code.`,
  config: { titleLines: 1, bodyLines: 2 },
  run(pass) {
    const { titleLines, bodyLines } = pass.config;
    for (const file of pass.files) {
      if (ast.isGenerated(file)) {
        continue;
      }
      for (const group of file.comments) {
        if (group === null) {
          continue;
        }
        const lines = group.list
          .flatMap((comment) => (comment === null ? [] : comment.text.split("\n")))
          .filter((line) => !directive.test(line))
          .map((line) => line.replace(/^\/\/\s?|^\/\*+|\*+\/$|^\s*\*\s?/g, "").trim());
        while (lines.length > 0 && lines[lines.length - 1] === "") {
          lines.pop();
        }
        const blank = lines.indexOf("");
        const title = blank === -1 ? lines.length : blank;
        const body = blank === -1 ? 0 : lines.length - blank - 1;
        if (title <= titleLines && body <= bodyLines) {
          continue;
        }
        pass.report({
          pos: group.pos(),
          end: group.end(),
          message: `shorten this comment to at most ${titleLines} summary line(s), then optionally a blank comment line and up to ${bodyLines} line(s) of explanation`,
        });
      }
    }
  },
});
