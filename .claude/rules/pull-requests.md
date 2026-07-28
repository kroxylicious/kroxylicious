# Pull Request Requirements

## Template

When creating pull requests, follow the checklist in [.github/PULL_REQUEST_TEMPLATE.md](../../.github/PULL_REQUEST_TEMPLATE.md).

## Key Points for Claude

- The template checklist is evaluated before merging, not necessarily before opening the PR
- Always include `Assisted-by:` trailer in commits (see commit conventions)
- For Sonarcloud warnings: either fix them or suppress with `@SuppressWarnings` plus justifying comment

## Changelog Entries

For user-facing changes, add a logchange YAML entry to `changelog/unreleased/`. See [DEV_GUIDE.md#changelog-entries](../../DEV_GUIDE.md#changelog-entries) for the format.
Do **not** edit `CHANGELOG.md` directly - it is regenerated at release time.
