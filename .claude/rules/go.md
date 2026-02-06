## Go (Golang) Code

- We are targeting Go 1.24 or newer.
- Use Go 1.22+'s new "for range" syntax everywhere possible.
- Combine multiple if clauses whose bodies do the same thing, into single expressions.
- Always use `any` rather than `interface{}`
- Use `github.com/alecthomas/errors` for errors if the project already uses it. It has `Errorf`, `New`, `Wrap`, etc.
- Always wrap errors, but try to be succinct if possible.
- Never use underscore in names.
- Use `github.com/alecthomas/assert/v2` for test assertions. In particular note that `assert.Equal()` performs a deep comparison.
- Prefer to compare whole objects rather than individual fields, using `assert.Equal(t, expected, actual, assert.Exclude[T]())` to exclude dynamic values like time.
- ALWAYS use table-driven tests if the tests can be parameterised on data. If not, just create distinct test functions.
- When writing "sub tests", their names MUST be UpperCamelCase.
- Test functions must always be UpperCamelCase, never with underscores.
- When writing code, avoid using `strings.Contains()` and string comparisons to compare types. Instead, use existing helper functions or methods, or write new ones.
- Where it makes sense, update existing test rather than creating new ones.
- ALWAYS run tests with `-timeout 30s` to ensure that wedged tests don't last forever.
- Don't run tests with `-v` in general, as it produces a large amount of output.
- Once the change is complete and working, run `golangci-lint run` and fix any linter errors introduced before adding the files to git. Do NOT EVER run `golangci-lint` on individual files.
- For "unparam" linter warnings about "XXX is unused", remove the parameter unless the type is part of an interface implementation or callback system.
- ALWAYS respect encapsulation of struct fields, even between types in the same package.
- ALWAYS apply the Go proverb "align the happy path to the left", to avoid deep nesting. 
    
    eg. instead of:
    ```go
    if a, ok := doA(); ok {
      if b, ok := doB(); ok {
      	// Code
      }
    }
    ```
    
    Do this:
    
    ```go
    a, ok := doA()
    if !ok {
    	continue // Or return
    }
    b, ok := doB()
    if !ok {
    	continue // Or return
    }
    ```
