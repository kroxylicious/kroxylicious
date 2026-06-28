# kroxylicious-kms Module

This module defines the Key Management System (KMS) API. See also [`../README.md`](../README.md) for project-wide context.

## API Roles

**KMS API Users/Callers**: Filters that call KMS methods to manage encryption keys. This includes `RecordEncryption`, but is not limited to that.

**KMS Implementers**: Developers writing KMS provider implementations that integrate with external key management systems (AWS KMS, HashiCorp Vault, Azure Key Vault, etc.).

---

# For KMS API Users (Filters)

This section describes how to use the KMS API when implementing filters that need key management.

## API Overview

The KMS API provides key management for encryption filters. It abstracts external key management systems behind a consistent interface.

**Core interface:** `Kms<C extends KmsConfig, K extends Kek, E>`

**Key methods you call:**

```java
CompletionStage<K> resolveKek(String kekSelector);
CompletionStage<EncryptedDek<E>> generateDekPair(K kek);
CompletionStage<Dek> decryptEdek(K kek, E edek);
```

## Using the API

**DEK (Data Encryption Key) lifecycle when calling the API:**

1. **Resolve KEK**: Call `resolveKek(selector)` to get the Key Encryption Key
2. **Generate DEK**: Call `generateDekPair(kek)` to create a new DEK and its encrypted form (edek)
3. **Encrypt data**: Use the plaintext DEK to encrypt record values
4. **Store edek**: Store the encrypted DEK (edek) alongside encrypted data
5. **Decrypt later**: Call `decryptEdek(kek, edek)` to recover the plaintext DEK for decryption

**Calling pattern:**
```java
// On first encryption
Kek kek = kms.resolveKek("my-kek-alias").toCompletableFuture().join();
EncryptedDek<E> dekPair = kms.generateDekPair(kek).toCompletableFuture().join();
byte[] ciphertext = encryptRecord(dekPair.dek(), record);
// Store dekPair.edek() with the ciphertext

// On decryption
Dek dek = kms.decryptEdek(kek, storedEdek).toCompletableFuture().join();
byte[] plaintext = decryptRecord(dek, ciphertext);
```

**DEK exhaustion**: DEKs have usage limits (number of encryptions). When you reach the limit, generate a new DEK:

```java
try {
    encryptRecord(currentDek, record);
    dekUsageCounter.increment();
} catch (DekUsageExhaustedException e) {
    // Generate new DEK
    currentDek = kms.generateDekPair(kek).toCompletableFuture().join();
    dekUsageCounter.reset();
}
```

## Error Handling When Calling

**Exception types you may encounter:**

- **`ExhaustedDekException`**: DEK has reached usage limit - generate a new DEK
- **`KmsException`**: General KMS error (network failure, authentication error) - retry or fail
- **`UnknownAliasException`**: KEK selector doesn't match any known KEK - configuration error

## Performance When Calling

**Caching:**

KMS calls are expensive (network latency, cryptographic operations). You should cache aggressively:

- **KEK cache**: Cache resolved KEKs with TTL (hours)
- **DEK cache**: Cache DEKs until exhaustion or TTL (minutes)
- **Connection pooling**: Reuse HTTP connections to KMS (implementation-specific)

**Async operations:**

Never block waiting for KMS responses:

```java
// ❌ BAD - blocks event loop
Kek kek = kms.resolveKek("alias").toCompletableFuture().join();

// ✅ GOOD - async composition
return kms.resolveKek("alias")
        .thenCompose(kek -> kms.generateDekPair(kek))
        .thenApply(dekPair -> encryptRecord(dekPair.dek(), record));
```

---

# For KMS Implementers

This section describes the requirements when implementing a KMS provider.

## Implementation Contracts

**Core interface:** `Kms<C extends KmsConfig, K extends Kek, E>`

Type parameters:
- `C`: Your configuration type (YAML-deserializable)
- `K`: The type of your Key Encryption Key (KEK) identifier (not the KEK itself).
- `E`: Your encrypted DEK type

**Methods you must implement:**

```java
CompletionStage<K> resolveKek(String kekSelector);
CompletionStage<EncryptedDek<E>> generateDekPair(K kek);
CompletionStage<Dek> decryptEdek(K kek, E edek);
```

**Requirements:**

- **Async, non-blocking**: All methods must return `CompletionStage` and never block event loop threads
- **Transient failure handling**: Implement retry with exponential backoff for transient failures
- **Opaque edeks**: Encrypted DEKs must be opaque to callers - never expose structure or allow deserialisation
- **Thread-safe**: Methods may be called from multiple event loop threads concurrently

## Resilience Patterns You Should Implement

- **Exponential backoff with jitter**: Retry transient failures with increasing delays
- **Circuit breaker**: Stop retrying after sustained failures to prevent cascade
- **Caching**: Cache KEKs and DEKs (with TTL) to reduce load on external KMS
- **Graceful degradation**: Continue with cached keys during temporary KMS outages (if security policy allows)

**Example retry implementation:**

```java
CompletionStage<K> resolveKek(String selector) {
    return kmsClient.getKey(selector)
            .handle((key, error) -> {
                if (error != null) {
                    if (isRetryable(error)) {
                        return retryWithBackoff(() -> kmsClient.getKey(selector));
                    }
                    throw new KmsException("Failed to resolve KEK", error);
                }
                return key;
            });
}
```

## Security Requirements for Implementations

**Key material handling:**

- **Plaintext DEKs**: Hold in memory only during encryption/decryption operations
- **Never log key material**: Don't log plaintext DEKs, KEKs, or sensitive edek details
- **Secure erasure**: Zero out key material after use (if language/platform supports)
- **Encrypted DEKs**: Make edeks opaque blobs; prevent deserialisation or structure inspection

**Authentication:**

- Your KMS connections must use TLS (never plaintext HTTP)
- Authenticate to external KMS using credentials from secure storage (not hardcoded)
- Support credential rotation

**Threat model:**

- **Compromised proxy**: Can access plaintext DEKs in memory during operation
- **Compromised KMS**: Can decrypt all edeks and derive plaintext DEKs
- **Network attacker**: Your TLS implementation must prevent eavesdropping on KMS communication
- **Key separation**: Support different KEKs for different tenants/topics to prevent cross-tenant decryption

## Testing Your Implementation

**Unit tests:**

Use `kroxylicious-kms-test-support` for isolated KMS testing:

```java
@Test
void testKekResolution() {
    var config = new MyKmsConfig(...);
    var kms = new MyKms();
    kms.initialize(config);

    var kek = kms.resolveKek("alias").toCompletableFuture().join();
    assertThat(kek).isNotNull();
}
```

**Integration tests:**

Test against real KMS instances (or test doubles):

```java
@IntegrationTest
class MyKmsIT {
    @RegisterExtension
    static TestKmsContainer kmsContainer = new TestKmsContainer();

    @Test
    void testDekLifecycle() {
        var kms = createKms(kmsContainer.getEndpoint());
        var kek = kms.resolveKek("test-kek").toCompletableFuture().join();
        var dekPair = kms.generateDekPair(kek).toCompletableFuture().join();

        // Encrypt with DEK
        byte[] plaintext = "secret".getBytes();
        byte[] ciphertext = encrypt(dekPair.dek(), plaintext);

        // Decrypt with recovered DEK
        var recoveredDek = kms.decryptEdek(kek, dekPair.edek()).toCompletableFuture().join();
        byte[] recovered = decrypt(recoveredDek, ciphertext);

        assertThat(recovered).isEqualTo(plaintext);
    }
}
```

## Registration

Your implementation must:
- Provide a `Kms` implementation class
- Define a configuration class implementing `KmsConfig`
- Register via `ServiceLoader` in `META-INF/services/io.kroxylicious.kms.service.Kms`
- Include integration tests demonstrating the full DEK lifecycle

---

# Reference Implementations

**Included KMS provider implementations:**

- **`kroxylicious-kms-provider-kroxylicious-inmemory`**: In-memory KMS for testing/development
- **`kroxylicious-kms-provider-hashicorp-vault`**: HashiCorp Vault integration
- **`kroxylicious-kms-provider-aws-kms`**: AWS KMS integration
- **`kroxylicious-kms-provider-azure-key-vault-kms`**: Azure Key Vault integration
- **`kroxylicious-kms-provider-fortanix-dsm`**: Fortanix DSM integration

Study these implementations for patterns and best practices.

## Cross-References

- **Security model**: See [`../README.md#security-model`](../README.md#security-model)
- **Filter API**: See [`../kroxylicious-api/README.md`](../kroxylicious-api/README.md)
- **Record encryption**: See `../kroxylicious-filters/kroxylicious-record-encryption/`
