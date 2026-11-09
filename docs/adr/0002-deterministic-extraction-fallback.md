# ADR 0002: Extract the classifier's category with a regex

**Status:** Accepted

## Context

The classifier is told to reply with labelled lines and nothing else. Its category was right across repeated tests, but it didn't hold the format: some replies added markdown, headers or a full explanation. Code that reads those replies can't depend on the format being exact.

## Options

| Option | For | Against |
|---|---|---|
| Keep tightening the prompt | No code | Lowers the failure rate but can't remove it |
| Have a second model call reformat the reply | Handles odd replies | More cost and latency, and it has the same problem |
| Parse the labels with a regex that tolerates formatting | Deterministic, testable, free | Only finds labels that are present |

## Decision

`parse_classification()` strips markdown markers and takes the first `Classification` or `Category` label naming a known category. Anything else raises `ValueError`, so a reply is never guessed at. The prompt keeps its strict output section because a clean reply is still the normal case.

The function was removed when `orchestrate.py` stopped reading the chain's output (commit `8b95442`). It's now back in `src/self_healing/journal.py`, where the results report uses it, and it has tests.

## Consequences

- The report records a reply it can't read as `unparsed` instead of failing or inventing a category.
- The raw reply stays in `evidence/run_journal.jsonl`, so any parse can be checked against it.
- A new label name needs a code change as well as a prompt change.
