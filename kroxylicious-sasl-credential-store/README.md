# SASL Credential Store API

Pluggable API for SCRAM credential storage used by SASL termination filters.

## Purpose

This module defines the API for credential stores that provide SCRAM credentials for SASL authentication. 
It provides an abstraction for calling code to authenticate clients against various backing stores without depending on specific implementations.
In practice the calling code is expected to be some kind of SASL terminating protocol filter, such as the `SaslTermination` filter. 

## Core Interfaces

### ScramCredentialStoreService

Service interface for credential store discovery and lifecycle management.

```java
public interface ScramCredentialStoreService<C> extends AutoCloseable {
    void initialize(C config);
    ScramCredentialStore buildCredentialStore();
    void close();
}
```

**Lifecycle:**
1. `initialize(config)` - Configure the service with type-safe configuration
2. `buildCredentialStore()` - Build credential store instances (may be called multiple times)
3. `close()` - Clean up resources

### ScramCredentialStore

Store interface for asynchronous credential lookups.

```java
public interface ScramCredentialStore {
    CompletionStage<ScramCredential> lookupCredential(String username);
}
```

**Returns:**
- A `ScramCredential` if the user exists
- `null` if the user does not exist
- Exceptional completion if the service is unavailable or times out

### ScramCredential

Immutable record storing SCRAM credential data.

```java
public record ScramCredential(
    String username,
    byte[] salt,           // Salt bytes (base64-encoded in JSON)
    int iterations,        // >= 4096
    byte[] serverKey,      // Server key bytes (base64-encoded in JSON)
    byte[] storedKey,      // Stored key bytes (base64-encoded in JSON)
    String hashAlgorithm   // "SHA-256" or "SHA-512"
) { }
```

**Validation:**
- All fields must be non-null and non-empty
- Iterations must be >= 4096
- Hash algorithm must be "SHA-256" or "SHA-512"

**Important:** Byte arrays are automatically encoded as base64 when serialised to JSON. The credential record provides defensive copies to prevent mutation.

## Exception Hierarchy

```
CredentialLookupException
├── CredentialServiceUnavailableException  (backing service unavailable)
└── CredentialServiceTimeoutException      (operation timed out)
```

## Implementing a Credential Store

1. Create a configuration class
2. Implement `ScramCredentialStoreService<YourConfig>`
3. Implement `ScramCredentialStore`
4. Register via Java ServiceLoader in `META-INF/services/io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService`

**Example structure:**
```
my-credential-store/
├── src/main/java/
│   └── com/example/
│       ├── MyCredentialStoreService.java
│       ├── MyCredentialStore.java
│       └── MyCredentialStoreConfig.java
└── src/main/resources/
    └── META-INF/services/
        └── io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService
```

## Using in a Filter

Filters use the `@Plugin` mechanism to discover credential stores:

```java
public record FilterConfig(
    @PluginImplName(ScramCredentialStoreService.class)
    String credentialStore,

    @PluginImplConfig(implNameProperty = "credentialStore")
    Object credentialStoreConfig
) {}
```

## Security Considerations

- **Never log credentials** - Credentials contain sensitive key material
- **Async operations** - All lookups must be non-blocking
- **Error handling** - Fail closed on service errors
- **Thread safety** - Stores may be called from multiple threads concurrently

## Dependencies

This module has minimal dependencies:
- `jackson-annotations` - JSON serialization annotations
- `spotbugs-annotations` - Nullability annotations

Test dependencies:
- `junit-jupiter-api` - Testing framework
- `assertj-core` - Assertions

## Implementations

Credential store implementations live in separate modules:
- `kroxylicious-sasl-credential-store-provider-keystore` - Java KeyStore-based implementation
- Additional implementations may be provided in the future (LDAP, database, etc.)

## See Also

- [KMS API](../kroxylicious-kms/) - Similar pluggable service pattern
- [SASL Termination Filter](../kroxylicious-filters/kroxylicious-sasl-termination/) - Primary consumer of this API
