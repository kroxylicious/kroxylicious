# SASL Termination Filter

Production-ready SASL termination filter that authenticates Kafka clients at the proxy layer.

## Overview

The SASL Termination filter enables the proxy to authenticate Kafka clients using SASL mechanisms without forwarding authentication requests to the broker. This provides:

- **Centralised authentication** - Authenticate clients at the proxy layer
- **Custom identity sources** - Integrate with LDAP, databases, KeyStore, or custom backends
- **Security barrier** - Block unauthenticated requests before they reach brokers
- **Independent mechanisms** - Use different SASL mechanisms between clients and brokers

## Supported Mechanisms

### SCRAM-SHA-256

Industry-standard salted challenge-response authentication mechanism (RFC 5802). Recommended for production use.

**Features:**
- Secure password-based authentication
- No plaintext password transmission
- Multi-round challenge-response protocol
- Protection against replay attacks

**Future mechanisms** (not yet implemented):
- SCRAM-SHA-512
- PLAIN
- OAUTHBEARER

## Configuration

### Basic Configuration

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
              password: "keystore-password"
            storeType: PKCS12
```

### Configuration with File-based Password

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
              file: /etc/kroxylicious/keystore-password.txt
            storeType: PKCS12
```

### Configuration Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `mechanisms` | map | Yes | Map of SASL mechanism names to their configurations |
| `mechanisms.<NAME>.credentialStore` | string | Yes | Fully-qualified class name or short name of credential store service |
| `mechanisms.<NAME>.credentialStoreConfig` | object | Yes | Configuration specific to the credential store implementation |

## How It Works

### Authentication Flow

1. **Client connects** - Kafka client connects to proxy
2. **API_VERSIONS** - Client discovers supported features
3. **SASL_HANDSHAKE** - Client and proxy negotiate SASL mechanism
4. **SASL_AUTHENTICATE** - Multi-round SCRAM exchange occurs
   - Client sends first message with username
   - Proxy looks up credentials asynchronously
   - Challenge-response rounds complete authentication
5. **Authentication result** - Success or failure
6. **Security barrier** - Only authenticated clients can proceed

### State Machine

```
START → RequiringHandshake → RequiringAuthenticate ↔ (challenge rounds)
                                       ↓
                               Authenticated | Failed
```

**Security barrier enforcement:**
- `API_VERSIONS`, `SASL_HANDSHAKE`, `SASL_AUTHENTICATE` allowed in any state
- All other requests require authenticated state
- Unauthenticated requests return `SASL_AUTHENTICATION_FAILED` and close connection

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
          file: /etc/kroxylicious/server-key-password.txt
```

### Credential Security

- **Never log credentials** - Credential stores must not log sensitive data
- **Fail closed** - Authentication failures result in connection closure
- **Timeout protection** - Credential lookups should timeout to prevent DOS
- **Strong iterations** - Use 10000+ PBKDF2 iterations for production

### Best Practices

- Combine with mTLS for defence-in-depth
- Use file-based passwords via `PasswordProvider`
- Restrict KeyStore file permissions (600)
- Monitor authentication metrics and audit logs
- Rotate credentials periodically

## Integration with Downstream Filters

### Subject Propagation

The filter calls `FilterContext.clientSaslAuthenticationSuccess()` on successful authentication, making the authenticated `Subject` available to downstream filters:

```java
// In downstream filter
Subject subject = filterContext.sslSession().authenticatedSubject();
Set<Principal> principals = subject.getPrincipals();
```

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

- **First connection** - 3-4 round trips for SCRAM-SHA-256
- **Subsequent requests** - No authentication overhead
- **No reauthentication** - KIP-368 not yet supported

## Troubleshooting

### "Authentication failed: Invalid client credentials"

**Possible causes:**
- Wrong password
- User doesn't exist in credential store
- Credential corruption in KeyStore
- Credential generation/storage mismatch

**Debug steps:**
1. Verify username exists: `keytool -list -keystore credentials.p12`
2. Check proxy logs for credential lookup errors
3. Verify credential format in KeyStore
4. Test with known-good credentials

### "Connection to node -1 failed authentication"

**Possible causes:**
- Client not configured for SASL
- SASL mechanism mismatch
- Security protocol configuration error

**Debug steps:**
1. Verify client configuration includes SASL settings
2. Check `SASL_MECHANISM` matches filter configuration
3. Ensure `SECURITY_PROTOCOL` is `SASL_PLAINTEXT` or `SASL_SSL`

### "SaslAuthenticationException: Authentication failed"

**Generic authentication failure** - Check proxy logs for specific error message.

## Example Client Configuration

### Java Producer

```java
Properties props = new Properties();
props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "proxy:9192");
props.put(ProducerConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"alice\" " +
    "password=\"alice-secret\";");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
```

### Java Consumer

```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "proxy:9192");
props.put(ConsumerConfig.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
props.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
props.put(SaslConfigs.SASL_JAAS_CONFIG,
    "org.apache.kafka.common.security.scram.ScramLoginModule required " +
    "username=\"bob\" " +
    "password=\"bob-secret\";");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
```

## Limitations

### Current Version

- **SCRAM-SHA-256 only** - Other mechanisms not yet implemented
- **No reauthentication** - KIP-368 support not included
- **Single mechanism per filter instance** - Cannot mix mechanisms
- **No credential caching** - Each authentication performs full lookup

### Future Enhancements

- SCRAM-SHA-512 support
- PLAIN mechanism support
- OAUTHBEARER support
- Reauthentication (KIP-368)
- Credential caching with TTL
- Rate limiting to prevent brute force

## Architecture

### Module Structure

```
kroxylicious-sasl-termination (filter)
    ↓ depends on
kroxylicious-sasl-credential-store (API)
    ↑ implemented by
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
- [Kafka SASL Documentation](https://kafka.apache.org/documentation/#security_sasl)
