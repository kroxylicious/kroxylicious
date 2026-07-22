# KeyStore SCRAM Credential Store

Java KeyStore-based implementation of the SCRAM credential store for SASL authentication.

## Overview

This module provides a production-ready credential store that loads SCRAM credentials from a Java KeyStore file. Credentials are loaded into memory at startup for fast, synchronous lookups during authentication.

## Features

- **SCRAM-SHA-256 and SCRAM-SHA-512 support** - Stores salted, hashed credentials
- **Java KeyStore integration** - Uses standard JKS or PKCS12 KeyStore formats
- **Secure credential storage** - Never stores plaintext passwords
- **Fast lookups** - In-memory cache for sub-millisecond credential retrieval
- **Password-protected** - KeyStore and individual keys protected by passwords

## Configuration

```yaml
credentialStore: KeystoreScramCredentialStore
credentialStoreConfig:
  file: /path/to/credentials.jks
  storePassword:
    password: "keystore-password"
  storeType: PKCS12
```

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `file` | string | Yes | - | Path to the KeyStore file |
| `storePassword` | PasswordProvider | Yes | - | Password for the KeyStore |
| `keyPassword` | PasswordProvider | No | Same as storePassword | Password for individual keys |
| `storeType` | string | No | Platform default | KeyStore type (e.g., "PKCS12", "JKS") |

## KeyStore Format

The KeyStore must contain `SecretKey` entries where:
- **Alias**: The username
- **Key bytes**: JSON-serialised `ScramCredential` data

### Credential JSON Format

```json
{
  "username": "alice",
  "salt": "FJz8jKVn7gKxR1wKGHXRXw==",
  "iterations": 4096,
  "serverKey": "yP4LXM+6b8SvL0N9i8fJQj5kJ5w=",
  "storedKey": "I8k2r9SvL0N9i8fJQj5kJ5wyP4L=",
  "hashAlgorithm": "SHA-256"
}
```

## Generating Credentials

### Using Java Code

```java
import io.kroxylicious.sasl.credentialstore.keystore.TestCredentialGenerator;

Path keystorePath = Paths.get("credentials.jks");
String password = "keystore-password";

var generator = new TestCredentialGenerator();
generator.generateKeyStore(
    keystorePath,
    password,
    "alice", "alice-secret",
    "bob", "bob-secret"
);
```

### Using Kafka's SCRAM Tools

You can also generate SCRAM credentials using Kafka's built-in tools and manually create the KeyStore:

```bash
# Generate SCRAM credentials using Kafka
kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-name alice \
  --add-config 'SCRAM-SHA-256=[password=alice-secret]'
```

Then extract the generated credentials and store them in the KeyStore format described above.

## Security Considerations

### KeyStore Protection

- **File permissions**: Restrict KeyStore file to 600 (owner read/write only)
- **Strong passwords**: Use strong, randomly generated passwords via `PasswordProvider`
- **Prefer PKCS12**: Modern PKCS12 format is more secure than legacy JKS

### Credential Security

- **No plaintext passwords**: Only salted, hashed credentials are stored
- **Sufficient iterations**: Default 4096 iterations (higher is more secure but slower)
- **Secure generation**: Use cryptographically random salts

### Operational Security

- **Rotate credentials**: Periodically update user passwords
- **Monitor access**: Log authentication attempts (success/failure)
- **Backup safely**: Encrypt KeyStore backups

## Limitations

### Current Version

- **Static credentials**: KeyStore is loaded once at startup; changes require restart
- **In-memory only**: All credentials held in memory (consider size for large user bases)
- **No hot reload**: Cannot dynamically reload credentials from file

### Future Enhancements

- File watching for dynamic credential updates
- Credential caching with TTL
- Support for remote KeyStore locations

## Example Usage

```yaml
filters:
  - type: SaslTermination
    config:
      mechanisms:
        SCRAM-SHA-256:
          credentialStore: KeystoreScramCredentialStore
          credentialStoreConfig:
            file: /etc/kroxylicious/credentials.p12
            storePassword:
              file: /etc/kroxylicious/keystore-password.txt
            storeType: PKCS12
```

## Troubleshooting

### "Failed to load KeyStore"

- **Check file path**: Ensure the file exists and is readable
- **Check password**: Verify storePassword is correct
- **Check format**: Ensure storeType matches actual KeyStore type

### "Failed to recover key for alias"

- **Check key password**: If keyPassword differs from storePassword, ensure it's correct
- **Check alias**: Verify username aliases match those in the KeyStore

### "Invalid credential for alias"

- **Check JSON format**: Ensure SecretKey bytes are valid JSON matching the schema
- **Check credential values**: Ensure all required fields are present and valid

## See Also

- [SASL Credential Store API](../../kroxylicious-sasl-credential-store/)
- [SASL Termination Filter](../../kroxylicious-filters/kroxylicious-sasl-termination/)
- [RFC 5802 - SCRAM](https://tools.ietf.org/html/rfc5802)
