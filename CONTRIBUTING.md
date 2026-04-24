# Contributing

Thank you for contributing to australian-kpis. This guide covers the workflow for humans; see [`Spec.md`](./Spec.md) for the architecture and [`AGENTS.md`](./AGENTS.md) for the condensed operating manual used by AI coding agents (PR rules, CI gates, pre-flight checks — applies to humans too).

## Development setup

Prerequisites:

- Rust 1.85+ (Rust 2024 edition support starts at 1.85; see
  `rust-toolchain.toml`)
- Bun 1.1+ or Node 20+ with pnpm 9
- Docker (for local infra)
- `sqlx-cli`, `cargo-nextest`, `cargo-deny`, `cargo-audit`, `cargo-llvm-cov`, `lefthook`

```bash
# Install Rust tooling
cargo install sqlx-cli cargo-nextest cargo-deny cargo-audit cargo-llvm-cov

# Install pre-commit hooks
brew install lefthook   # or: npm install -g lefthook
lefthook install

# Spin local infra
docker compose -f infra/compose/docker-compose.yml up -d

# Run migrations
sqlx migrate run

# Run tests
cargo nextest run --workspace
```

`lefthook install` wires a `pre-commit` hook that runs `cargo fmt --check`,
`biome format --check`, `gitleaks protect --staged`, and a staged-file size
guard that rejects blobs over 5 MB. The hook body is shell/Python only, so it
works the same way on macOS and Linux.

## Workflow

1. Pick an issue from the [milestone board](https://github.com/ponderingdemocritus/australian-kpis/milestones). Respect `Depends on:` — don't start a blocked issue.
2. Branch: `ponderingdemocritus/<issue-number>-<short-slug>`. Use concrete nouns; keep under 30 chars.
3. Work in small, reviewable PRs — one PR per issue unless otherwise noted.
4. Every PR must satisfy the issue's **Pass requirements** checklist and all CI gates (see `Spec.md § CI/CD pipeline`).
5. Link the PR to the issue via `Closes #N` in the description.

## Commit style

- Conventional Commits-ish: `type(scope): subject` (e.g., `feat(adapter-abs): implement streaming SDMX parse`)
- Types: `feat`, `fix`, `refactor`, `test`, `ci`, `docs`, `chore`, `security`
- Keep commits small; prefer rebase over merge on your branch

## Code style

- **Rust**: `cargo fmt` + `cargo clippy -- -D warnings`. No `unwrap()` in non-test code; use `?` + `thiserror`.
- **TS**: `biome format` + `biome lint`. Prefer `type` over `interface` for unions; export from index barrels only when tree-shaking holds.
- **SQL**: compile-checked via `sqlx::query!`; run `cargo sqlx prepare` before committing queries.
- **Comments**: default to none. Only write one when the *why* is non-obvious.

## Testing

See `Spec.md § Testing strategy`. Minimum:

- Every public fn has at least one test
- `pnpm run lint` passes locally
- `cargo nextest run --workspace` passes locally
- Coverage ≥80% line / ≥70% branch (PR bot comments with diff)
- For adapter changes: add/update `insta` snapshots with real fixtures
- For SQL changes: include a migration + integration test

The repo-level test workflow and command references live in
[`docs/testing.md`](./docs/testing.md).

## Flake policy

Zero tolerance. Any retry in CI auto-files a `flaky`-labelled issue. Fix within 48h or delete the test.

## Security

See `Spec.md § Security posture`. Never commit secrets; `gitleaks` runs pre-commit and in CI. Report vulnerabilities privately to the maintainer (see `SECURITY.md` once created).
