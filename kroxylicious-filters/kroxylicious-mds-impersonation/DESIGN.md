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

For each Kafka connection, initial `ApiVersions` v0 discovery verifies that both
SASL APIs support v1. Unsupported brokers fail with an explicit reason/code.
The filter then initiates upstream `SaslHandshake` v1 and
`SaslAuthenticate` v1 with OAUTHBEARER before forwarding any client request.
The broker verifies the MDS token and applies the user's RBAC/ACLs. The configured
target cluster must use TLS and the MDS token validator; it can additionally
require the proxy's client certificate. This filter does not grant broker rights.

## Lifecycle and failures

MDS HTTPS I/O is asynchronous and bounded by a timeout and response-size limit.
TLS peer and hostname verification are required, and redirects are disabled.
The shared HTTP client admits at most 16 requests, with no waiting queue.
Service failures trigger jittered exponential backoff (500 ms to 5 seconds) and
a single recovery probe; principal-specific denials do not block other users.
A bounded four-thread executor has 256 internal task slots. The factory closes
both resources. Metrics record token latency/outcome, admission rejections,
initial/renewal authentication results and closure reasons, without user labels.
Logs retain reason codes, numeric HTTP/Kafka errors, exception types and optional
sanitized stack frames, never external messages or cause chains.
Authentication failures close the Kafka connection without forwarding the
pending request or disclosing tokens or remote error bodies.

Tokens and authentication state are per connection. No cross-user token cache is
introduced. Every response must contain a textual JWT `sub` exactly matching the
requested user; missing, mismatched or duplicate claims fail closed before SASL,
including during renewal. The JWT `exp` claim is used only to bound connection lifetime, not to
authenticate the client; the token comes from trusted MDS over TLS and its
signature is validated by the broker. The earlier of JWT expiry and a positive
broker `sessionLifetimeMs`, less a configurable safety margin, is the renewal
deadline. When the broker advertises a positive session lifetime, the next request
at or after that deadline obtains a fresh token and performs KIP-368
`SaslHandshake` / `SaslAuthenticate` on the same upstream connection. The mapped
user must remain unchanged. One pending authentication stage is shared by requests
on that connection. The filter returns an incomplete result, using the runtime's
existing backpressure, ordering and timeout; it does not maintain a payload queue.
The runtime correlates generated SASL responses independently of application
responses already in flight. Zero-ack Produce requests do not require a response
or special draining logic.
Place this filter last, nearest the broker: a filter upstream of it could otherwise
release delayed requests or generate new ones in the middle of the SASL exchange.
Downstream-generated requests also pass through the authentication gate.

There are no periodic renewal tasks or retained request contexts between requests.
An idle connection reauthenticates before its next application request, including
after its former session expires: Kafka handles a reauthentication handshake before
checking application request expiry. Normal network/broker idle timeouts still apply.
Tokens must remain usable throughout the exchange and the new renewal deadline
must allow at least one second between handshake starts (Kafka's minimum interval).
MDS, broker or timeout failures close the connection without forwarding held traffic;
there is no fallback to the previous token or another principal. A zero broker
session lifetime retains the original close-on-JWT-expiry policy because that broker
does not advertise reauthentication support. No public filter API or YAML changes
are required.

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
moving the reusable HTTP TLS helper out of the KMS module,
certificate reload, and MDS endpoint failover. An MDS HA endpoint
can be configured for this initial implementation.
