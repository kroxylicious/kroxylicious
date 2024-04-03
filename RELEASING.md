# Releasing Guide for Kroxylicious

This document describes how to release this component.

The component is released using GitHub automation.

At a high level, the process is as follows:

1. The developer adds their private key/passphrase as repository secrets
1. The workflow `stage_release` tags, builds/signs the release, and stages the release on a Nexus staging repository. This process uses the GitHub machine account [kroxylicious-robot](https://github.com/kroxylicious-robot) and a user token owned by Sonatype account `kroxylicious` account.
1. The stage release is verified using manual verification steps.
1. The workflow `deploy_release` releases from the staged repository to Maven Central.
1. The developer removes their private key/passphrase as repository secrets.

## Pre-Requisites

You must be a member of the Kroxylicious organization and have access to [create 
secrets](https://github.com/kroxylicious/kroxylicious/settings/secrets/actions) within the kroxylicious repository.

Create the following repository secrets:

| Secret                                        | Description                                                                |
|-----------------------------------------------|----------------------------------------------------------------------------|
| `KROXYLICIOUS_RELEASE_PRIVATE_KEY`            | Private key, in armor format, of the project admin conducting the release. |
| `KROXYLICIOUS_RELEASE_PRIVATE_KEY_PASSPHRASE` | Passphrase used to protect the private key                                 |

To export your key run something like
```shell
gpg --armor --export-secret-key ${KEY_ID} | pbcopy
```

While `pbcopy` is macOS specific, similar utilities exist for Linux see [StackExchange](https://superuser.com/a/288333)

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
* stage a release [Nexus UI](https://s01.oss.sonatype.org/). It'll be named `iokroxylicious-nn`.

### Verify the Release

You can validate the staged artefacts by having a test application (we'll call it `T`) that uses kroxylicious use the Maven artefacts by making
temporary (local) changes to its POM.

1. Find the staging repository in the [Nexus UI](https://s01.oss.sonatype.org/). It'll be named `iokroxylious-nn`.
1. Add a [`<repositories>`](https://maven.apache.org/pom.html#Repositories) that references the staging repository public url to `T`'s POM.
1. Update `T`'s kroxylicious dependency to refer to the `<RELEASE_VERSION>`.
1. Run `T` build/test cycle but use an alternative cache location to be sure artefacts are being fetched.  Check the build output, you'll see the
   kroxylicious comes from the staging location.
```bash
MAVEN_OPTS="-Dmaven.repo.local=/tmp/repository" mvn verify
```
If the build passes, proceed to make the release public.
The local changes made to `T`'s POM can be reverted.

### Making the release public

1. Run [deploy_workflow](https://github.com/kroxylicious/kroxylicious/actions/workflows/deploy_release.yaml)
   setting the `next-state` to `release` to publish the artefact and publish the release notes.
1. Merge release PR (use Rebase and Merge strategy).
1. Let [Kroxylicious Team Developers](https://kroxylicious.slack.com/archives/C04V1K6EAKZ) know the release is finished.

If anything goes wrong, follow the steps in the next section.

### Failed Releases

If the release fails verification, use the [deploy_workflow](https://github.com/kroxylicious/kroxylicious/actions/workflows/deploy_release.yaml) with  the `drop` argument.
This will drop the snapshot repository and delete the release notes. Manually close the PR.

### Remove your private key/passphrase

Remove your private key/passphrase secrets from the
[repository secrets](https://github.com/kroxylicious/kroxylicious/settings/secrets/actions).



