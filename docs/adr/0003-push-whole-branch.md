# ADR 0003: apply_patch pushes the whole current branch

**Status:** Accepted, as a known risk

## Context

Databricks jobs run from a workspace Git folder, so a patch has to reach the remote before the workspace can pull it. `apply_patch` commits the change and runs `git push origin <branch>`, which sends every local commit on the branch, not only the new one.

## Options

| Option | For | Against |
|---|---|---|
| Push the current branch | One command; the workspace pulls exactly what was pushed | Unreviewed commits already on the branch go with it |
| Create a new branch per patch and open a PR | Reviewed and isolated | The workspace folder must switch branches for each verification, and it adds PR handling to the loop |
| Refuse to push when the branch is ahead of its remote | Small change, removes the surprise | Breaks the flow whenever there's legitimate local work |

## Decision

Keep the push of the current branch. This is a single-contributor demo, and the approval prompt happens right before it.

## Consequences

- On a shared branch this would be wrong. Local commits could be published as a side effect of approving a patch.
- `old_snippet` must match exactly once and the path must be inside the repo, but neither check limits what the push sends.
- For shared use, the fix is a per-patch branch and a pull request, as described in [the design notes](../design.md#security).
