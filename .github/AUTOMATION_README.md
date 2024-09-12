# GitHub Automation

## Shared GitHub Personal Access Tokens

In some of our actions we use Personal Access Tokens (PAT) to enable actions
to work with GitHub APIs with fine-grained permissions beyond what the action
has in its default token.

These tokens must belong to an account, so we use a shared account to hold them. This account
is [kroxylicious-robot](https://github.com/kroxylicious-robot) and credentials are shared by some committers, ask in
#team-developers
on slack if you require a new PAT for a GitHub action.

## Dependabot Integration

We use Dependabot to automatically create Pull Requests (PR) upgrading dependencies. Dependabot is also allowed to merge
those PRs via comments on the PR (enabling us to queue up dependency merges). However, the actions triggered by the
Dependabot actor have a different security scope than user initiated actions. The Dependabot actions cannot use Secrets
that user initiated actions can, you have to create a separate set of Secrets at organisation or repo level. See the
[docs](https://docs.github.com/en/code-security/dependabot/working-with-dependabot/configuring-access-to-private-registries-for-dependabot).
In practice this means we have some duplicated secrets across `Actions` and `Dependabot`.
