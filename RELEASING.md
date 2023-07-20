= Releasing Kroxylicious

A quick and dirty guide to actually releasing builds for consumption via maven central.

## Requirements
### Sonatype/Maven Central

* A Maven Central/Sonatype account. Accounts are created via their https://issues.sonatype.org/secure/Signup!default.jspa[JIRA signup]
* Your Sonatype account needs to be granted publication permissions for the Kroxylicous project.
** One of the existing team members with publishing rights needs to raise a https://issues.sonatype.org/secure/CreateIssue.jspa?pid=10134&issuetype=11003[JIRA ticket] to request the new account is added (or removed...). The requirements for the ticket are listed in the https://central.sonatype.org/publish/manage-permissions/[Central Docs]

Signing the builds with a GPG key is a requirement for publishing to Maven Central.

### PGP
* Ensure your PGP key is published to a public key server (for example keys.openpgp.org)
* Get your PGP key signed by the Kroxylicious project key. Rather than share a single key between project members (which has issues with revocation and re-issuing) we sign the releases individually using a key signed by the project key as a mark of trust.
* A copy of your keys short ID.

TIP: To get the short ID for your key `gpg2 --show-keys --keyid-format short $\{YOUR_KEY_LONG_ID}`

### Development environment

* Once logged in to the https://s01.oss.sonatype.org/[Sonatype Nexus UI], create a User Token and configure this in the https://maven.apache.org/settings.html#servers[`<servers>`] element o  `.m2/settings.xml`.  This is required in order to allow Maven to push the artifacts to Nexus. 
* The release scripts will make use of the GitHub CLI https://cli.github.com/ if present, so you may wish to install to reduces the number of chores.


## Running a release

The project is release is two parts, the API and the Framework. There are separate release scripts beneath `./scripts` for each of these.

```shell
scripts/release-api.sh <RELEASE_VERSION>
```

```shell
scripts/release-framework.sh <RELEASE_VERSION>
```


.Releasing via the build server
**TBD**

Specifically we need to work out the commit rights and key signing for the build server.
