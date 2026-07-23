# SASL Termination Filter

Production-ready SASL termination filter that authenticates Kafka clients at the proxy layer.

## Overview

The SASL Termination filter enables the proxy to authenticate Kafka clients using SASL mechanisms without forwarding authentication requests to the broker. This provides:

- **Centralised authentication** - Authenticate clients at the proxy layer
- **Custom identity sources** - Integrate with LDAP, databases, KeyStore, or custom backends (SCRAM), or validate JWT tokens against a JWKS endpoint (OAUTHBEARER)
- **Security barrier** - Block unauthenticated requests before they reach brokers
- **Independent mechanisms** - Use different SASL mechanisms between clients and brokers
- **Reauthentication (KIP-368)** - Enforce session lifetimes with configurable reauthentication

## Supported Mechanisms

### SCRAM-SHA-256

Industry-standard salted challenge-response authentication mechanism (RFC 5802). Recommended for password-based authentication.

**Features:**
- Secure password-based authentication
- No plaintext password transmission
- Multi-round challenge-response protocol
- Protection against replay attacks

### SCRAM-SHA-512

Same as SCRAM-SHA-256 but uses the SHA-512 hash algorithm, providing stronger cryptographic properties at the cost of slightly more computational resources.

### OAUTHBEARER

JWT-based token authentication (RFC 7628). Validates bearer tokens against a JWKS endpoint provided by your identity provider.

**Features:**
- Token-based authentication using JWT bearer tokens
- Signature verification against JWKS endpoint
- Audience and issuer validation
- Token expiry drives reauthentication
- No credential store required

## Configuration

### SCRAM Configuration

```yaml
filters:
  - type: SaslTermination
    config:
      mechanisms:
        SCRAM-SHA-256:
          credentialStore: KeystoreScramCredentialStoreService
          credentialStoreConfig:
            file: /etc/kroxylicious/credentials.p12
            storePassword:
              passwordFile: /etc/kroxylicious/keystore-password.txt
            storeType: PKCS12
```

### OAUTHBEARER Configuration

```yaml
filters:
  - type: SaslTermination
    config:
      mechanisms:
        OAUTHBEARER:
          jwksEndpointUrl: https://idp.example.com/.well-known/jwks.json
          expectedAudience: kafka
          expectedIssuer: https://idp.example.com
```

### Mixed Mechanisms with Reauthentication

```yaml
filters:
  - type: SaslTermination
    config:
      maxTimeBeforeReauth: 1h
      mechanisms:
        SCRAM-SHA-256:
          credentialStore: KeystoreScramCredentialStoreService
          credentialStoreConfig:
            file: /etc/kroxylicious/credentials.p12
            storePassword:
              passwordFile: /etc/kroxylicious/keystore-password.txt
            storeType: PKCS12
        OAUTHBEARER:
          jwksEndpointUrl: https://idp.example.com/.well-known/jwks.json
          expectedAudience: kafka
          expectedIssuer: https://idp.example.com
```

### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mechanisms` | map | Yes | Map of SASL mechanism names to their configurations. At least one entry required. |
| `maxTimeBeforeReauth` | duration | No | Maximum session lifetime before reauthentication is required (KIP-368). Duration syntax (e.g. `1h`, `30m`, `3600s`). |

**SCRAM mechanism config:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `credentialStore` | string | Yes | Plugin name of credential store service (e.g. `KeystoreScramCredentialStoreService`) |
| `credentialStoreConfig` | object | Yes | Configuration specific to the credential store implementation |

**OAUTHBEARER mechanism config:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `jwksEndpointUrl` | URI | Yes | URL of the JWKS endpoint for fetching token signing keys |
| `expectedAudience` | string | Yes | Expected `aud` claim value (comma-separated for multiple) |
| `expectedIssuer` | string | Yes | Expected `iss` claim value |
| `scopeClaimName` | string | No | JWT claim name for scope (default: `scope`) |
| `subClaimName` | string | No | JWT claim name for subject (default: `sub`) |
| `jwksEndpointRefreshMs` | long | No | JWKS endpoint refresh interval in milliseconds |
| `jwksEndpointRetryBackoffMs` | long | No | Initial retry backoff in milliseconds |
| `jwksEndpointRetryBackoffMaxMs` | long | No | Maximum retry backoff in milliseconds |

## How It Works

### Authentication Flow

1. **Client connects** - Kafka client connects to proxy
2. **API_VERSIONS** - Client discovers supported features
3. **SASL_HANDSHAKE** - Client and proxy negotiate SASL mechanism
4. **SASL_AUTHENTICATE** - Authentication exchange occurs
   - *SCRAM:* Multi-round challenge-response with credential lookup
   - *OAUTHBEARER:* Single-round JWT token validation against JWKS endpoint
5. **Authentication result** - Success or failure
6. **Security barrier** - Only authenticated clients can proceed

### State Machine

```
START -> RequiringHandshake -> RequiringAuthenticate <-> (challenge rounds)
                                       |
                               Authenticated | Failed
                                       |
                          (reauthentication via new SASL_HANDSHAKE)
```

**Security barrier enforcement:**
- `API_VERSIONS`, `SASL_HANDSHAKE`, `SASL_AUTHENTICATE` allowed in any state
- All other requests require authenticated state
- Unauthenticated requests return `SASL_AUTHENTICATION_FAILED` and close connection

### Reauthentication (KIP-368)

The filter supports KIP-368 reauthentication. The effective session lifetime is the minimum of:
- The configured `maxTimeBeforeReauth` value
- The mechanism-reported credential lifetime (e.g. JWT token expiry for OAUTHBEARER)

Standard Kafka clients (4.0+) handle reauthentication transparently. If a session expires without reauthentication, the proxy rejects the next non-SASL request and closes the connection.

## Security Considerations

### Transport Security

**Always use TLS** - SASL should be used over TLS to protect authentication metadata:

```yaml
virtualClusters:
  demo:
    tls:
      key:
        storeFile: /etc/kroxylicious/server-key.p12
        storePassword:
          passwordFile: /etc/kroxylicious/server-key-password.txt
```

### Credential Security

- **Never log credentials** - Credential stores must not log sensitive data
- **Fail closed** - Authentication failures result in connection closure
- **Timeout protection** - Credential lookups should timeout to prevent DOS
- **Strong iterations** - Use 10000+ PBKDF2 iterations for production (SCRAM)

### OAUTHBEARER Security

- **Audience and issuer validation** - Both are required to prevent cross-service token acceptance
- **JWKS endpoint trust** - Ensure the endpoint is served over HTTPS
- **TLS limitation** - The JVM's default trust store is used for JWKS endpoint communication; custom trust stores are not configurable

### Best Practices

- Combine with mTLS for defence-in-depth
- Use file-based passwords via `PasswordProvider` (SCRAM)
- Restrict KeyStore file permissions (600)
- Monitor authentication metrics and audit logs
- Rotate credentials periodically

## Integration with Downstream Filters

### Subject Propagation

The filter calls `FilterContext.clientSaslAuthenticationSuccess()` on successful authentication, making the authenticated `Subject` available to downstream filters.

### Audit Logging

Authentication events are automatically logged through the audit system:

- **Successful authentication** - Action: `ClientSaslAuthenticationSuccess`
- **Failed authentication** - Action: `ClientSaslAuthenticationFailure`

## Performance Characteristics

### Credential Lookup

- **KeyStore provider** - In-memory lookups (sub-millisecond)
- **Future providers** - May have higher latency (LDAP, database)
- **Async design** - Non-blocking credential lookups

### Connection Overhead

- **SCRAM** - 3-4 round trips for initial authentication
- **OAUTHBEARER** - Typically single round trip
- **Subsequent requests** - No authentication overhead until reauthentication
- **Reauthentication** - Transparent for Kafka 4.0+ clients

## Troubleshooting

### "Authentication failed: Invalid client credentials"

**Possible causes:**
- Wrong password (SCRAM)
- User doesn't exist in credential store (SCRAM)
- Invalid or expired JWT token (OAUTHBEARER)
- Token audience or issuer mismatch (OAUTHBEARER)

**Debug steps:**
1. Check proxy logs for specific error messages
2. For SCRAM: verify username exists in keystore (`keystore-credential-tool list-users -k credentials.p12`)
3. For OAUTHBEARER: verify token claims match `expectedAudience` and `expectedIssuer` configuration
4. Test with known-good credentials or tokens

### "Connection to node -1 failed authentication"

**Possible causes:**
- Client not configured for SASL
- SASL mechanism mismatch
- Security protocol configuration error

**Debug steps:**
1. Verify client configuration includes SASL settings
2. Check `SASL_MECHANISM` matches filter configuration
3. Ensure `SECURITY_PROTOCOL` is `SASL_SSL`

## Example Client Configuration

### Java Producer (SCRAM)

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "proxy:9192");
props.put(ProducerConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"alice\" " +
    "password=\"alice-secret\";");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

### Java Producer (OAUTHBEARER)

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "proxy:9192");
props.put(ProducerConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
    "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler");
props.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL,
    "https://idp.example.com/oauth/token");
props.put(SaslConfigs.SASL_OAUTHBEARER_SCOPE_CLAIM_NAME, "scope");
props.put("clientId", "my-kafka-client");
props.put("clientSecret", "client-secret");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

## Limitations

### Current Version

- **No SASL PLAIN support** - PLAIN transmits passwords in cleartext and is unsuitable without TLS
- **No GSSAPI (Kerberos) support** - Kerberos has fundamentally different operational requirements
- **No credential caching** - KeyStore provider loads all credentials into memory at startup; no TTL-based caching
- **No hot reloading** - Credential changes require proxy restart or virtual cluster reconfiguration
- **No rate limiting** - No built-in brute-force protection for failed authentication attempts
- **No custom TLS for JWKS** - OAUTHBEARER JWKS endpoint uses JVM default trust store

### Future Enhancements

- PLAIN mechanism support
- Rate limiting to prevent brute force
- Credential caching with TTL for external credential stores
- Hot reloading of credential stores

## Architecture

### Module Structure

```
kroxylicious-sasl-termination (filter)
    | depends on
kroxylicious-sasl-credential-store (API)
    ^ implemented by
kroxylicious-sasl-credential-store-provider-* (providers)
```

### Extensibility Points

**User-facing (@Plugin):**
- `ScramCredentialStoreService` - Custom credential backends

**Internal (ServiceLoader):**
- `MechanismHandlerFactory` - Additional SASL mechanisms

## See Also

- [SCRAM Credential Store API](../../kroxylicious-sasl-credential-store/)
- [KeyStore Provider](../../kroxylicious-sasl-credential-store-providers/kroxylicious-sasl-credential-store-provider-keystore/)
- [RFC 5802 - SCRAM](https://tools.ietf.org/html/rfc5802)
- [RFC 7628 - OAUTHBEARER](https://tools.ietf.org/html/rfc7628)
- [Kafka SASL Documentation](https://kafka.apache.org/documentation/#security_sasl)
