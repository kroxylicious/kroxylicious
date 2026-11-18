# Releasing Guide for Kroxylicious

This document describes how to release this component.

The component is released using GitHub automation.

At a high level, the process is as follows:

1. The developer prepares the release blog post.
1. The developer adds their private key/passphrase as repository secrets
1. The workflow `stage_release` tags, builds/signs the release, and stages the release on a Maven Central Portal. This process uses the GitHub machine account [kroxylicious-robot](https://github.com/kroxylicious-robot) and a user token owned by Sonatype account `kroxylicious` account.
1. The staged release is verified using manual verification steps.
1. The release is made public.
1. The developer removes their private key/passphrase from the repository secrets.

## Prerequisites

You must be a member of the Kroxylicious [release-engineers](https://github.com/orgs/kroxylicious/teams/release-engineers) and have access to [create 
secrets](https://github.com/kroxylicious/kroxylicious/settings/secrets/actions) within the kroxylicious repository.

You must be [registered](https://central.sonatype.org/register/central-portal/) with Sonatype Central Portal  and your id must be registered as [publisher](https://central.sonatype.com/publishing/io.kroxylicious/users) for the Kroxylicious namespace. To do the latter step, ask someone who is a publisher to [open a support ticket](https://central.sonatype.org/faq/what-happened-to-issues-sonatype-org/#i-used-to-registerupdate-my-ossrh-account-at-issuessonatypeorg-what-do-i-do-now:~:text=To%20add%20a%20new%20publisher%20to%20an%20existing%20namespace%2C).

You will need a GPG key, follow this [guide](https://help.ubuntu.com/community/GnuPrivacyGuardHowto#Generating_an_OpenPGP_Key).

You will need to upload your GPG **public** key to some keyservers. You can follow [this](https://help.ubuntu.com/community/GnuPrivacyGuardHowto#Uploading_the_key_to_Ubuntu_keyserver) which explains how to obtain your public key. Upload that key to the following keyservers:
- https://keyserver.ubuntu.com/
- https://keys.openpgp.org/upload
- http://pgp.mit.edu:11371/

Create-or-update the following repository secrets:

| Secret                                        | Description                                                                                                                                           |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `KROXYLICIOUS_RELEASE_PRIVATE_KEY`            | Private key, in armor format, of the project admin conducting the release.                                                                            |
| `KROXYLICIOUS_RELEASE_PRIVATE_KEY_PASSPHRASE` | Passphrase used to protect the private key                                                                                                            |

Note that we depend on the following tokens that belong to the `kroxylicious-robot` user, defined at the Organization level (see [AUTOMATION_README.md](.github/AUTOMATION_README.md):
| Secret                                        | Description                                                                                                                                           |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| `KROXYLICIOUS_RELEASE_TOKEN`                  | GitHub PAT with write permissions for content, commit status, pull-requests, workflows for this repository and the kroxylicious.github.io repository             |


To export your key run something like
```shell
gpg --armor --export-secret-key ${KEY_ID} | pbcopy
```

While `pbcopy` is macOS specific, similar utilities exist for Linux see [StackExchange](https://superuser.com/a/288333)

## Prepare the release blog post

Prepare the release blog post by opening a PR [kroxylicious.github.io](https://github.com/kroxylicious/kroxylicious.github.io).  Get the PR
reviewed by your peers, addressing any comments, until the content is agreed.  Don't merge it yet.

## Release steps

Use the [Kroxylicious Team Developers](https://kroxylicious.slack.com/archives/C04V1K6EAKZ) Slack Channel to coordinate
the release with the other developers.  It is important no other work is merged to main during the release.

### Stage the Release

Run [stage_workflow](https://github.com/kroxylicious/kroxylicious/actions/workflows/stage_release.yaml).
Set the `release-version` argument to the version being release e.g. `0.7.0`.

This will:

* raise single PR that will contain two commits:
  1. the first will version the artefacts at `release-version`.  A `release-version` tag will point at this commit.
  2. the second will re-open main for development, at the next snapshot.
* stage a deployment in the [Central Publishing Portal](https://central.sonatype.com/publishing).

If anything goes wrong, follow the steps in [Failed Releases](#failed-releases)

### Verify the Release

You can validate the staged artefacts by using a test application, `T`, use the Maven artefacts.   The [kroxylicious-wasm](https://github.com/andreaTP/kroxylicious-wasm) from the
[community-gallery](https://github.com/kroxylicious/kroxylicious-community-gallery) is a suitable choice.

1. [Configure Maven](https://central.sonatype.org/publish/publish-portal-api/#manually-testing-a-deployment-bundle) to download staged artefacts from Central Publishing Portal.
   To do this, you need to [generate a user token](https://central.sonatype.org/publish/publish-portal-api/#authentication-authorization) for your Sonatype account and then [configure maven]
   (https://central.sonatype.org/publish/publish-portal-api/#manually-testing-a-deployment-bundle) to use it.
   
1. Run `T` build/test cycle but use an alternative cache location to be sure artefacts are being fetched.  Check the build output, you'll see the
   kroxylicious comes from the staging location.
```bash
MAVEN_OPTS="-Dmaven.repo.local=/tmp/repository" mvn verify -Dkroxylicious.version=<new release version>
```
If the build passes, proceed to make the release public.
The local changes made to `T`'s POM can be reverted.

1. Once you've finished verifying the release, [revoke](https://central.sonatype.com/account) your Sonatype token.

### Making the release public

1. Comment on the PR `@kroxylicious-robot promote-release`.
1. Let [Kroxylicious Team Developers](https://kroxylicious.slack.com/archives/C04V1K6EAKZ) know the release is finished.
1. [Publish](https://github.com/kroxylicious/kroxylicious.github.io/blob/main/docs/README.md) the documentation for the release
1. Merge the blog post PR
1. Post to social media about the release.

If anything goes wrong, follow the steps in [Failed Releases](#failed-releases)

### Failed Releases

If the release fails verification, comment on the PR `@kroxylicious-robot drop-release`.
This will drop the snapshot repository, delete the release notes and close PR.

### Remove your private key/passphrase

Update the private key/passphrase secrets from the
[repository secrets](https://github.com/kroxylicious/kroxylicious/settings/secrets/actions) to whitespace.

## Container Image Tags

Images are published to the same registry and organisation (`vars.REGISTRY_SERVER` / `vars.REGISTRY_ORGANISATION`)
regardless of whether the build is a snapshot push, a staged release, or a promoted one - there's no separate
registry for pre-release images.

**Snapshot pushes:** every successful push to `main` or a release branch builds and pushes images tagged
`commit-<SHA>` and `<version>-SNAPSHOT`. The `-SNAPSHOT` tag is overwritten on each push while the POM version is
unchanged, so it's a floating tag: it reflects the most recently *successful* build, not necessarily the branch's
current HEAD, and a failed build simply leaves it pointing at an older commit. Pull request builds are never pushed.

**Staging (`stage_release`):** dispatches the image build directly against the release commit, producing
`commit-<SHA>` tags only - no version tag exists until promotion.

**Promotion (`promote_release`):** checks the staged manifests exist, then re-tags the exact staged images (via
`skopeo copy --all`, with no rebuild) as the release version (e.g. `0.25.0`) and as `latest`. The staged
`commit-<SHA>` tag is not deleted on a successful promotion - only `drop-release` deletes staged images, as cleanup
for an abandoned release. So after a release, both the version tag and the originating `commit-<SHA>` tag persist
and point at identical content, which is what gives a released version traceability back to its exact source
commit.

`latest` is the one tag we deliberately keep floating past its initial publish, re-pointed at the newest release
each time, matching what users conventionally expect of it.

**Known limitation:** nothing in the workflow actually stops two different builds from writing different content to
the same `commit-<SHA>` tag. In practice this hasn't happened, because `promote-release`'s `gh pr merge --merge`
always produces a two-parent merge commit, so the commit `stage_release` tags is never the same SHA that an
ordinary push-triggered build later tags - the staged commit stays reachable from the branch but never becomes a
tip that CI rebuilds. That's a byproduct of the current merge topology, not an enforced guarantee.

## Creating a Release Branch

Run [create-release-branch](https://github.com/kroxylicious/kroxylicious/actions/workflows/create-release-branch.yaml)
to create a release branch from an existing release tag.

| Input | Description | Example |
|---|---|---|
| `release-tag` | The existing release tag to branch from | `v0.25.0` |
| `development-version` | The next patch SNAPSHOT version. Omit to derive automatically (patch + 1). | `0.25.1-SNAPSHOT` |

This creates a branch named `release/v<major>.<minor>` (e.g. `release/v0.25`) and opens it for
development at the next SNAPSHOT version by updating all Maven POMs and the non-Maven version
references (docs attributes, container image tags, benchmark defaults).

