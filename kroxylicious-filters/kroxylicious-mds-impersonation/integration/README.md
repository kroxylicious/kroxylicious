# Real Confluent MDS integration tests

This disposable environment runs Confluent Platform 8.3.1 with MDS and a single
combined KRaft broker/controller. Kroxylicious and unmodified Java Kafka clients
run in the Maven test JVM on the host. No deployed cluster is required.

## Start the environment

Prerequisites: Docker Compose, Python 3, OpenSSL, Java 21 (including `keytool`),
and the repository's Maven build prerequisites. Use Linux with available loopback
ports 18090, 19092, 19093 and 19096. The broker heap is limited to 768 MiB; allow
additional memory for the broker process and Maven. The image includes components
under the [Confluent Enterprise License](https://docs.confluent.io/platform/current/installation/docker/image-reference.html#confluent-server-image),
including the RBAC functionality used here. Review the applicable terms before
running it locally or enabling it in a repository's CI.

From this directory:

```shell
python3 generate.py
docker compose -p kroxy-mds-integration config --quiet
docker compose -p kroxy-mds-integration up -d
python3 provision.py
```

Provisioning waits for MDS, grants alice access to topic `mds-allowed` and group
`mds-test`, and creates the topic with two partitions. To use a different Compose
project name, also pass `--project-name NAME` to `provision.py`.

From the repository root, install the filter's dependencies once, then run the
unit, protocol, and opt-in Confluent tests:

```shell
mvn -pl :kroxylicious-mds-impersonation -am -Dquick install
mvn -pl :kroxylicious-mds-impersonation \
  '-Dtest=Mds*Test,MdsConfluent*IT' -Dmds.integration=true test
```

The generated configuration path defaults to `integration/generated` relative to
the module. Override it with `-Dmds.integration.dir=/absolute/path` if necessary.
The real Confluent tests are disabled without `-Dmds.integration=true`.
Run them serially because they share fixed listener ports and a consumer group.

The `Confluent MDS integration tests` workflow is opt-in. A maintainer can use
`workflow_dispatch`, or add the `ci/mds-integration` label to a PR that changes
the MDS module, its selected dependencies, or the workflow/build configuration
listed in the workflow's `paths` filter. Adding another label does not run the
job. Pushes and PR updates do not start it automatically; remove and re-add the
label to test an updated PR revision. Manual dispatch can test a selected branch
regardless of the changed paths. GitHub only offers manual dispatch once the
workflow is present on the repository's default branch.

When requested, the workflow performs the dependency build, environment startup,
provisioning and test command. It uploads an
`mds-test-evidence` artifact on both success and failure, containing the tested
commit/tree, tool versions, image digests, JUnit XML, a suite summary and file
hashes. JVM properties and captured test output are removed from the exported
XML; generated credentials are not included. The job fails if a real MDS test suite
is missing, skipped or unsuccessful. GitHub may require a maintainer to
approve workflow execution for an external contributor's PR.

After a local test run, `python3 integration/collect-reports.py` from the module
directory exports the JUnit reports and summary to `target/mds-evidence/`.
The GitHub workflow additionally records the revision, tools and image digest.

To run on a remote Docker host, copy the checkout to a dedicated directory on
that host and run these same commands through SSH there, including Maven. This
keeps broker addresses, generated file paths and test clients in the same host
network. Merely changing `DOCKER_HOST` does not copy bind-mounted files to the
daemon host. Select Java 21 for the test shell if the host's default Java is older;
there is no need to change the system-wide Java selection.

## What the tests exercise

* Client certificates map to alice or bob at the proxy; both use the same Kafka
  client ID. Alice can produce and consume; bob receives a topic authorization
  error from the real broker.
* MDS rejects impersonation of `User:ANONYMOUS`, which is configured as a protected
  user. No user allow-list is configured in the proxy.
* Two alice consumers rebalance two partitions while records are produced and
  consumed through the expiry of short-lived MDS tokens.
* One producer and consumer exchange records for 75 seconds, followed by a
  40-second producer idle period and another exchange. Their connection creation
  and closure counters must remain unchanged across these 30-second token lifetimes.
  The same producer must still receive a topic authorization error for an
  unauthorized topic after renewal.
* The mock-wire tests check that authentication gates both client and
  filter-generated requests, that in-flight responses remain correlated during
  reauthentication, and that queued requests including zero-ack Produce keep their
  order. MDS receives the proxy certificate; the broker connection remains open.

For an isolated deployment with different ports, test clients accept
`-Dmds.integration.bootstrap=localhost:PORT` and
`-Dmds.integration.mds=https://localhost:PORT`.
Set the corresponding addresses in the generated proxy configuration, broker
advertised listeners, and Compose port mappings as well. These overrides do not
modify a running environment or allocate ports automatically.

The trust domains are separate. The proxy trusts the client CA. MDS trusts the
proxy CA and permits `User:proxy` impersonation. The token broker listener uses
`SASL_SSL/OAUTHBEARER` with the MDS signing public key and `ssl.client.auth=none`;
it does not request or trust a proxy client certificate. The proxy validates both
MDS and broker server certificates.

This environment does not exercise multiple brokers, coordinator failover, VRFs,
gateway failover, or the Kubernetes operator.

## Lifecycle

Certificates expire after two days and generated keys/configuration are ignored
by Git. MDS tokens live for 30 seconds to exercise expiry quickly. The internal
PLAINTEXT listener and anonymous superuser exist only for local provisioning on
the Compose network; these settings are unsuitable for deployment. Published
broker/MDS ports bind only to loopback. Do not attach untrusted containers to this
test network.

Stop only this test project when finished:

```shell
docker compose -p kroxy-mds-integration down
```

Kafka data is in tmpfs, so rerun `provision.py` after a broker restart. To regenerate
expired credentials, stop the project, remove only this directory's `generated/`
and `.env`, and repeat the startup steps. `generate.py` refuses to overwrite an
existing credential directory.
