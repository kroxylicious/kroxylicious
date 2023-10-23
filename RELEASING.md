# Releasing Kroxylicious

A quick and dirty guide to actually releasing builds for consumption via maven central.

## Requirements
### Sonatype/Maven Central

* A Maven Central/Sonatype account. Accounts are created via their [JIRA signup](https://issues.sonatype.org/secure/Signup!default.jspa)
* Your Sonatype account needs to be granted publication permissions for the Kroxylicous project.
** One of the existing team members with publishing rights needs to raise a [JIRA ticket](https://issues.sonatype.org/secure/CreateIssue.jspa?pid=10134&issuetype=11003) to request the new account is added (or removed...). The requirements for the ticket are listed in the [Central Docs](https://central.sonatype.org/publish/manage-permissions/)

Signing the builds with a GPG key is a requirement for publishing to Maven Central.

### PGP
* Ensure your PGP key is published to a public key server (for example keys.openpgp.org)
* Get your PGP key signed by the Kroxylicious project key. Rather than share a single key between project members (which has issues with revocation and re-issuing) we sign the releases individually using a key signed by the project key as a mark of trust.
* A copy of your keys short ID.

TIP: To get the short ID for your key `gpg2 --show-keys --keyid-format short ${YOUR_KEY_LONG_ID}`

### Development environment

* Once logged in to the [Sonatype Nexus UI](https://s01.oss.sonatype.org/), create a User Token and configure this in the [`<servers>`](https://maven.apache.org/settings.html#servers) element in  `.m2/settings.xml`.  This is required in order to allow Maven to push the artifacts to Nexus. 
* The release scripts will make use of the GitHub CLI https://cli.github.com/ if present, so you may wish to install to reduces the number of chores.


## Running a release

Use the release script to actually perform the release and prepare main for the next development version.

```shell
./scripts/release.sh -k <YOUR_KEY_SHORT_ID> -v <RELEASE_VERSION> [-b <BRANCH_FROM>] [-r <REPOSITORY>]
```

where
* `<YOUR_KEY_SHORT_ID>` is the short id of your PGP key 
* `<RELEASE_VERSION>` is a release number such as 0.6.0
* `<BRANCH_FROM>` is the branch to release (defaults to `main`)
* `<REPOSITORY>` is the remote name of the kroxylicious repository (defaults to `origin`)

Once the release is completed, use the [Nexus UI](https://s01.oss.sonatype.org/) to close the staging repository, then release it. That will push the Maven artefacts available
on Maven Central.  The artefacts will take a while to reach all the Maven Central mirrors.

## Releasing via the build server
**TBD**

Specifically we need to work out the commit rights and key signing for the build server.
