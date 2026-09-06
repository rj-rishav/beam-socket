# Security

## Reporting a vulnerability

Email **rj-rishav** via GitHub Security Advisories (private disclosure):
<https://github.com/rj-rishav/beam-socket/security/advisories/new>.

Do not file public issues for suspected vulnerabilities.

## 2026-09-06 — Secret rotation + history scrub

During early development (commit `bf286d4`, 2026-07-04), a local
`.env`, a Vim swap file (`.env.swp`), and `.claude/settings.json` were
committed to this repository. Those files contained Anthropic API
credentials used during single-developer experimentation.

**Status:**

- The affected credentials were **revoked at the provider** on 2026-09-06
  before this notice was written.
- Git history was scrubbed with `git-filter-repo` on 2026-09-06 across
  all local branches. The pre-filter commit hash was `b98dce8`; the
  post-filter HEAD is `a8568d9`. All branch refs were rewritten.
- `.gitignore` now excludes `.env`, `.env.*` (except `.env.example`),
  swap files, and `.claude/settings*.json`.
- A pre-commit guard in `scripts/git-hooks/pre-commit` (registered via
  `git config core.hooksPath scripts/git-hooks`) blocks any commit whose
  staged diff matches a known credential pattern.
- This notice is the durable record; the offending commit hashes no
  longer exist in this repository.

**Action for forks:** anyone who forked the repo before 2026-09-06 has
a copy of the original history. Treat any Anthropic credentials
present in such forks as compromised and rotate them. Force-pull this
repository's rewritten history if you depend on the same commits.

**Action for collaborators:** pull with `--force` (or re-clone) and
re-establish any local branches from the rewritten remote refs.
