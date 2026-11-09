# ADR 0004: The bundle declares jobs, but they run from the Git folder

**Status:** Accepted

## Context

A Databricks Asset Bundle normally uploads the code and points jobs at that copy. This project's repair loop relies on a different path: `apply_patch` pushes to Git, updates the workspace Git folder, and confirms that folder is at the pushed commit before a re-run.

If jobs ran from the bundle's copy, an approved patch wouldn't change what the job executes until someone deployed again.

The Free Edition workspace is also shared by every target, so dev and prod would write to the same tables unless they're separated.

## Options

| Option | For | Against |
|---|---|---|
| Jobs use the bundle's uploaded files; `apply_patch` runs `bundle deploy` | The usual bundle pattern | Replaces a sync that is confirmed by commit hash, and needs the CLI inside the MCP server |
| Jobs use the Git folder, set by a bundle variable | Job definitions become declarative and the repair loop doesn't change | Less common, and the bundle doesn't ship the code |

## Decision

`databricks.yml` declares all jobs, and each one runs `${var.git_folder}/src/databricks_entry.py`. The bundle uploads no files. `dev` and `prod` differ by Git folder, job name prefix and schema (`self_healing_demo.pipeline` or `self_healing_demo.pipeline_prod`).

Job ids are no longer stored in a generated `job_ids.json`. The MCP server looks jobs up by exact name and refuses zero or several matches.

## Consequences

- Deploying changes job definitions. Code changes reach a target when its Git folder updates, which `deploy.yml` does after each deploy.
- Each target needs its Git folder to exist before its jobs can run.
- Prod is gated by a GitHub environment approval. It shares a workspace with dev, so it shows the promotion path but not real isolation.
