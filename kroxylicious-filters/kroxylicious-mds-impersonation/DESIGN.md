# Draft: mTLS to Confluent MDS impersonation

This is an experimental implementation proposal for discussion, related to
[authentication swapping #2166](https://github.com/kroxylicious/kroxylicious/issues/2166).
It needs upstream design review before inclusion in a supported release.
It uses the existing TLS identity and filter APIs; it does not change their contracts.

## Authentication boundary

Unmodified Kafka clients authenticate to the proxy with mTLS. The configured
transport subject builder maps the verified certificate to a `User`. The filter
requires a client certificate and a valid mapped user name, rejects downstream
SASL, and never derives identity from client IDs, addresses, or Kafka requests.

The proxy calls MDS `POST /security/1.0/impersonate` with that user as
`targetPrincipalName` and `User` as `targetPrincipalType`. It authenticates this
HTTPS request with its own provisioned client certificate. MDS must permit that
proxy principal to impersonate users through `confluent.metadata.server.impersonation.super.users`
and restricts targets with `confluent.metadata.server.impersonation.protected.users`.
The proxy passes authenticated user names to MDS without maintaining a local user list.
The proxy does not create certificates or
copy clients' private keys. MDS and broker trust configuration are independent.

For each Kafka connection, the filter initiates upstream `SaslHandshake` v1 and
`SaslAuthenticate` v1 with OAUTHBEARER before forwarding any client request.
The broker verifies the MDS token and applies the user's RBAC/ACLs. The configured
target cluster must use TLS and the MDS token validator; it can additionally
require the proxy's client certificate. This filter does not grant broker rights.

## Lifecycle and failures

MDS HTTPS I/O is asynchronous and bounded by a timeout and response-size limit.
TLS peer and hostname verification are required, and redirects are disabled.
Authentication failures close the Kafka connection without forwarding the
pending request or disclosing tokens or remote error bodies.

Tokens and authentication state are per connection. No cross-user token cache is
introduced. The JWT `exp` claim is used only to bound connection lifetime, not to
authenticate the client; the token comes from trusted MDS over TLS and its
signature is validated by the broker. The earlier of JWT expiry and a positive
broker `sessionLifetimeMs`, less a configurable safety margin, is the deadline.
At the next request after that deadline the connection closes without forwarding
that request. Idle connections are not proactively closed. A reconnect fetches a
new token. In-flight operations can fail and must use the client's normal retry
policy. Transparent KIP-368 reauthentication is deferred.

## Routing scope

This draft targets direct routing (one upstream per downstream connection).
Each broker/coordinator connection authenticates separately, including after
metadata refresh and consumer rebalance. Existing endpoint rewriting remains the
runtime's responsibility. Dynamic routers that multiplex upstream connections
need per-upstream authentication and are outside this draft.

VRFs belong to deployment: run separate proxy instances/network namespaces for
overlapping addresses. This filter neither selects VRFs nor resolves ambiguous
same-process client routes. Network namespaces, DNS, TLS names and advertised
gateway endpoints must still be configured consistently.

## Review and validation

Unit and protocol tests cover identity isolation, authentication ordering,
denial, expiry, HTTP limits, and reconnects. The opt-in Docker Compose environment
uses Confluent Platform 8.3.1 with real MDS, separate certificate trust domains,
and a single KRaft broker/controller. Tests verify alice's produce/consume access,
bob's RBAC denial with the same client ID, MDS rejection of a protected user, and two consumers rebalancing and
delivering records across 30-second token expiry. The broker does not request a
proxy client certificate. For reproduction, see [integration/README.md](integration/README.md).

Multi-broker coordinator/leader changes, VRF routing, gateway failover, RBAC audit
log identity, and a complete Operator deployment remain unvalidated. The existing
filter API does not expose upstream TLS/routing policy: the operator must enforce
server-verified TLS and direct routing in the virtual cluster configuration.

Open design questions include upstream acceptance of a vendor-specific filter,
moving the reusable HTTP TLS helper out of the KMS module, transparent upstream
reauthentication, certificate reload, and MDS endpoint failover. An MDS HA endpoint
can be configured for this initial implementation.
