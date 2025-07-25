# GitHub Automation

## Password Safe

The project uses a password safe as the system of record for project's secrets.  The [CODEOWNERS](./CODEOWNERS) know the
whereabout of the password safe and how to access it.  Talk to one of them if you need access.

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

## Sonatype / Maven Central

In order to release to Maven Central, the Kroxylicious Robot has an account at https://central.sonatype.com/.
This is used by the release stage and promote workflows.
The credentials for the Kroxylicious Robot are in the 1Password safe.

The workflows themselves authenticate using a Sonatype *user token* belonging to the  Kroxylicious Robot.  There's no
expiration on the token.

To refresh the token:

1. Login to https://central.sonatype.com/ as the Kroxylicious Robot.
2. Navigate to "View Account".
3. Select "Generate user token"
4. Store the username and password at the Github organisational level in variable/secret `KROXYLICIOUS_SONATYPE_TOKEN_USERNAME` and `KROXYLICIOUS_SONATYPE_TOKEN_PASSWORD`

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

## Deploying the Snapshot Website to GitHub Pages on a Fork

We have automation to deploy a snapshot version of the website (source at https://github.com/kroxylicious/kroxylicious.github.io) with the latest documentation
from this repository incorporated. This is deployed to https://kroxylicious.io/kroxylicious so that we can see what the documentation in this repository looks
like when combined with the website, testing the API between the two repos.

To exercise the GitHub workflows and share documentation changes it can be convenient to deploy this to your own fork. To do this you need to add some configuration
so that:
1. Actions can deploy to GitHub Pages
2. The built HTML refers to your github pages URL as it will be hosted at `https://<your-name>.github.io/kroxylicious/`. Jekyll builds up absolute URLs using a
   configured site url.

To enable pages on your fork:
1. go to `https://github.com/${yourname}/kroxylicious.github.io/settings` in a browser, replacing `${yourname}` with your GitHub username.
2. Navigate to "Pages" under "Code and automation"
3. Under "Build and deployment", under "Source", select "Github Actions".
4. Navigate to "Actions" under "Secrets and variables" under "Security"
5. Select the "Variables" tab
6. Click "New repository variable"
7. Create a new repository variable named `JEKYLL_CONFIG_OVERRIDES` with value:
   ```yaml
   url: "https://${yourname}.github.io"
   ```
   replacing `${yourname}` with your GitHub username.
8. Push changes to any branch of your fork and then trigger a manual run of `https://github.com/${yourname}/kroxylicious/actions/workflows/publish-snapshot-docs-to-website.yaml`.
   supplying the branch you want to checkout and deploy as a parameter. 
