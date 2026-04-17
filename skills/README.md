# Skills

Agent Skills shipped with this repo. Each subdirectory is a self-contained
skill with a `SKILL.md` and (optional) `scripts/`, `references/`, `assets/`.

## Skills in this repo

- [`blueprint-author/`](blueprint-author/) — compose, review, and debug
  resolution-engine blueprints. Routes to the node catalog, per-executor
  schemas, and the validator diagnostics.

## Install

Skills are auto-discovered by Claude Code and OpenAI Codex CLI when dropped
into their skills directories. They use the same `SKILL.md` format.

### If you want a skill in your user profile (all projects)

Copy the skill folder into your tool's home skills directory.

```bash
# Claude Code
cp -r skills/blueprint-author ~/.claude/skills/

# OpenAI Codex CLI
cp -r skills/blueprint-author ~/.agents/skills/
```

### If you want a skill in a different project

Copy into that project's local skills directory.

```bash
# Claude Code in <other-project>/
cp -r skills/blueprint-author <other-project>/.claude/skills/

# Codex CLI in <other-project>/
cp -r skills/blueprint-author <other-project>/.agents/skills/
```

Skills link to docs via absolute GitHub URLs, so the references resolve
from any install location given network access.

### Verify

Both tools auto-detect newly installed skills. Restart your session if a
skill doesn't appear. Claude Code's slash menu and Codex CLI's skill list
should show `blueprint-author`.

## Authoring your own skill

See Anthropic's [Agent Skills](https://docs.claude.com/en/docs/claude-code/skills)
guide and OpenAI's [Agent Skills for Codex](https://developers.openai.com/codex/skills)
documentation. Both tools share the same on-disk format.
