# GitHub Automation

## Shared GitHub Personal Access Tokens

In some of our actions we use Personal Access Tokens (PAT) to enable actions
to work with GitHub APIs with fine-grained permissions beyond what the action
has in its default token.

These tokens must belong to an account, so we use a shared account to hold them. This account
is [kroxylicious-robot](https://github.com/kroxylicious-robot) and credentials are shared by some committers, ask in #team-developers
on slack if you require a new PAT for a GitHub action.

## Dependabot Integration

We use Dependabot to automatically create Pull Requests (PR) upgrading dependencies. Dependabot is also allowed to merge
those PRs via comments on the PR (enabling us to queue up dependency merges). However, the actions triggered by the
Dependabot actor have a different security scope than user initiated actions. The Dependabot actions cannot use Secrets
that user initiated actions can, you have to create a separate set of Secrets at organisation or repo level. See the 
[docs](https://docs.github.com/en/code-security/dependabot/working-with-dependabot/configuring-access-to-private-registries-for-dependabot).
In practice this means we have some duplicated secrets across `Actions` and `Dependabot`.

## Fortanix DSM Integration Tests

In order for CI to execute the Fortanix DSM Integration Tests, a Fortanix DSM SaaS account has been created.  kroxylicious-admin@.. is an
adminstrator in that account.  Kroxylicious Developers will find the credentials in the team password safe.

The account has two "Apps" that are used for CI purposes "Github CI (Facade/Admin)"  and "Github CI (KMS)".  These are configured for API Key
authentication.    The API Keys are set as two Github secrets: `KROXYLICIOUS_KMS_FORTANIX_ADMIN_API_KEY` and `KROXYLICIOUS_KMS_FORTANIX_API_KEY`
within https://github.com/kroxylicious/kroxylicious/settings/secrets/actions.   In addition there is a Github variable `KROXYLICIOUS_KMS_FORTANIX_API_ENDPOINT`
which points at the endpoint of the Fortanix DSM SaaS services.  This is set at https://github.com/kroxylicious/kroxylicious/settings/variables/actions. The CI
workflows turn those Github secrets/variable into environment variables (for the same name) for the Maven runs.

The Fortanix DSM SaaS account is a long-lived account so should not expire.  When discussing the account with the Fortanix, quote the account id
found on this page https://uk.smartkey.io/#/settings.

## Jenkins integration

In order to be able to run the system and performance tests for our Pull Requests (PR) using Jenkins, we have defined the following Personal Access Token (PAT) 
`KROXYLICIOUS_JENKINS_TOKEN` to allow `kroxylicious-robot` writing comments with the results of the test execution and adding a new status check in the PR.

