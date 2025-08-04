# Releasing Guide for Kroxylicious

This document describes how to release this component.

The component is released using GitHub automation.

At a high level, the process is as follows:

1. The developer prepares the release blog post.
1. The developer adds their private key/passphrase as repository secrets
1. The workflow `stage_release` tags, builds/signs the release, and stages the release on a Nexus staging repository. This process uses the GitHub machine account [kroxylicious-robot](https://github.com/kroxylicious-robot) and a user token owned by Sonatype account `kroxylicious` account.
1. The stage release is verified using manual verification steps.
1. The release is made public.
1. The developer removes their private key/passphrase from the repository secrets.

## Pre-Requisites

You must be a member of the Kroxylicious [release-engineers](https://github.com/orgs/kroxylicious/teams/release-engineers) and have access to [create 
secrets](https://github.com/kroxylicious/kroxylicious/settings/secrets/actions) within the kroxylicious repository.

You will need a GPG key, follow this [guide](https://help.ubuntu.com/community/GnuPrivacyGuardHowto#Generating_an_OpenPGP_Key).

You will need to upload your GPG **public** key to some keyservers. You can follow [this](https://help.ubuntu.com/community/GnuPrivacyGuardHowto#Uploading_the_key_to_Ubuntu_keyserver) which explains how to obtain your public key. Upload that key to the following keyservers:
- https://keyserver.ubuntu.com/
- https://keys.openpgp.org/upload
- http://pgp.mit.edu:11371/

Create-or-update the following repository secrets:

| Secret                                        | Description                                                                                                                               |
|-----------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `KROXYLICIOUS_RELEASE_PRIVATE_KEY`            | Private key, in armor format, of the project admin conducting the release.                                                                |
| `KROXYLICIOUS_RELEASE_PRIVATE_KEY_PASSPHRASE` | Passphrase used to protect the private key                                                                                                |
| `KROXYLICIOUS_RELEASE_TOKEN`                  | GitHub PAT with write permissions for content, commit status, pull-requests for this repository and the kroxylicious.github.io repository |


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

1. [Configure Maven](https://central.sonatype.org/publish/publish-portal-api/#verify-status-of-the-deployment) to download staged artefacts from Central Publishing Portal.
1. Run `T` build/test cycle but use an alternative cache location to be sure artefacts are being fetched.  Check the build output, you'll see the
   kroxylicious comes from the staging location.
```bash
MAVEN_OPTS="-Dmaven.repo.local=/tmp/repository" mvn verify -Dkroxylicious.version=<new release version>
```
If the build passes, proceed to make the release public.
The local changes made to `T`'s POM can be reverted.

### Making the release public

1. Comment on the PR `@kroxylcious-robot promote-release`.
1. Let [Kroxylicious Team Developers](https://kroxylicious.slack.com/archives/C04V1K6EAKZ) know the release is finished.
1. [Publish](https://github.com/kroxylicious/kroxylicious.github.io/blob/main/docs/README.md) the documentation for the release
1. Merge the blog post PR
1. Post to social media about the release.

If anything goes wrong, follow the steps in [Failed Releases](#failed-releases)

### Failed Releases

If the release fails verification, comment on the PR `@kroxylcious-robot drop-release`.
This will drop the snapshot repository, delete the release notes and close PR.

### Remove your private key/passphrase

Update the private key/passphrase secrets from the
[repository secrets](https://github.com/kroxylicious/kroxylicious/settings/secrets/actions) to whitespace.



