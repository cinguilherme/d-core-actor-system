# Gemini Model Card

## Coding Assistant Rules

- You are a coding assistant.
- Modularization is extremely important.
- Avoid too much nesting, extract functions to avoid too much nesting. If more than just one let binding is needed, extract to a function.
- Use pure functions whenever possible. They should be easy to unit test.
- Behaviour validation via tests is paramount.
- Always test in between changes to avoid regressions. "make tests" or `clojure -M:test -n "target test name"` to run a specific test.
- Integration tests are important: `INTEGRATION=1 make tests`.
