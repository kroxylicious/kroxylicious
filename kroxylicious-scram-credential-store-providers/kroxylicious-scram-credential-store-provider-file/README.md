# SCRAM Credential File Store

File-based implementation of the SCRAM credential store for SASL authentication.

## Overview

This module provides a production-ready credential store that loads SCRAM credentials from a proxy SCRAM credential file (PKCS12 format). Credentials are loaded into memory at startup for fast, synchronous lookups during authentication.

## Features

- **SCRAM-SHA-256 and SCRAM-SHA-512 support** - Stores salted, hashed credentials
- **PKCS12 file format** - Uses standard PKCS12 KeyStore format
- **Secure credential storage** - Never stores plaintext passwords
- **Fast lookups** - In-memory cache for sub-millisecond credential retrieval
- **Password-protected** - Credential file protected by password

## Configuration

```yaml
credentialStore: ScramCredentialFileService
credentialStoreConfig:
  file: /path/to/credentials.p12
  storePassword:
    passwordFile: /etc/kroxylicious/keystore-password.txt
```

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `file` | string | Yes | - | Path to the proxy SCRAM credential file |
| `storePassword` | PasswordProvider | Yes | - | Password for the credential file |

## Credential File Format

The credential file must contain `SecretKey` entries where:
- **Alias**: The lowercase hex SHA-256 hash of the username (UTF-8 encoded). Using a hash prevents
  leaking usernames if the file is inspected directly.
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

### Using the Credential Tool

The module includes a CLI tool for managing credentials in proxy SCRAM credential files.

#### Building the Distribution

The credential tool is bundled into the main proxy distribution. Build it with:

```bash
mvn package -Pdist -DskipTests
```

The distribution archive is created under `kroxylicious-app/target/` in tar.gz, zip, and exploded directory formats.

#### Running the Tool

From the exploded proxy distribution directory:

```bash
# Show usage
bin/scram-credential-tool.sh --help

# Create a new credential file
bin/scram-credential-tool.sh create -k credentials.p12

# Add a user (prompts for passwords interactively)
bin/scram-credential-tool.sh add-user -k credentials.p12 -u alice

# List users
bin/scram-credential-tool.sh list-users -k credentials.p12

# Update a user's password
bin/scram-credential-tool.sh update-password -k credentials.p12 -u alice

# Remove a user
bin/scram-credential-tool.sh remove-user -k credentials.p12 -u alice
```

By default, passwords are read interactively from the console.
Use `--unlock-insecure-options` to enable command-line password arguments (`-p`, `-w`), but note this is **not recommended** as passwords become visible in process listings and shell history.

### Using Kafka's SCRAM Tools

You can also generate SCRAM credentials using Kafka's built-in tools and manually create the credential file:

```bash
# Generate SCRAM credentials using Kafka
kafka-configs --bootstrap-server localhost:9092 \
  --alter --entity-type users --entity-name alice \
  --add-config 'SCRAM-SHA-256=[password=alice-secret]'
```

Then extract the generated credentials and store them in the credential file format described above.

## Security Considerations

### Credential File Protection

- **File permissions**: Restrict credential file to 600 (owner read/write only)
- **Strong passwords**: Use strong, randomly generated passwords via `PasswordProvider`
- **PKCS12 format**: Uses the modern PKCS12 KeyStore format

### Credential Security

- **No plaintext passwords**: Only salted, hashed credentials are stored
- **Sufficient iterations**: Default 10000 iterations (higher is more secure but slower)
- **Secure generation**: Use cryptographically random salts

### Operational Security

- **Rotate credentials**: Periodically update user passwords
- **Monitor access**: Log authentication attempts (success/failure)
- **Backup safely**: Encrypt credential file backups

## Limitations

### Current Version

- **Static credentials**: Credential file is loaded once at startup; changes require restart
- **In-memory only**: All credentials held in memory (consider size for large user bases)
- **No hot reload**: Cannot dynamically reload credentials from file

### Future Enhancements

- File watching for dynamic credential updates
- Credential caching with TTL
- Support for remote credential file locations

## Example Usage

```yaml
filters:
  - type: SaslTermination
    config:
      mechanisms:
        SCRAM-SHA-256:
          credentialStore: ScramCredentialFileService
          credentialStoreConfig:
            file: /etc/kroxylicious/credentials.p12
            storePassword:
              file: /etc/kroxylicious/keystore-password.txt
```

## Troubleshooting

### "Failed to load credential file"

- **Check file path**: Ensure the file exists and is readable
- **Check password**: Verify storePassword is correct

### "Failed to recover key for alias"

- **Check password**: Verify the store password is correct
- **Check alias**: Verify username aliases match those in the credential file

### "Invalid credential for alias"

- **Check JSON format**: Ensure SecretKey bytes are valid JSON matching the schema
- **Check credential values**: Ensure all required fields are present and valid

## See Also

- [SCRAM Credential Store API](../../kroxylicious-scram-credential-store/)
- [SASL Termination Filter](../../kroxylicious-filters/kroxylicious-sasl-termination/)
- [RFC 5802 - SCRAM](https://tools.ietf.org/html/rfc5802)
