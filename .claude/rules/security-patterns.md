# Security Coding Rules

## Fail-Safe Defaults

**Authentication:**
- Never throw exceptions that close connections unexpectedly
- Anonymous fallback on authentication failure
- Subject construction must be defensive

**Authorisation:**
- Fail-closed semantics: deny by default
- Only allow explicitly permitted actions
- On error or exception, always deny (never allow)

**Pattern:**
```java
// ✅ Correct fail-closed
return policyEngine.evaluate(subject, action, resource)
        .exceptionally(error -> {
            logger.error("Authorisation failed", error);
            return AuthoriseResult.deny(); // Deny on error
        });

// ❌ Wrong - allows on error
return policyEngine.evaluate(subject, action, resource)
        .exceptionally(error -> AuthoriseResult.allow());
```

## Key Material Handling

**Never log or expose plaintext key material:**
- DEKs (Data Encryption Keys)
- KEKs (Key Encryption Keys)
- Passwords
- TLS private keys
- API tokens

**Pattern:**
```java
// ❌ BAD
logger.debug("Using key: {}", plaintextKey);

// ✅ GOOD
logger.debug("Using key with ID: {}", keyId);
```

## TLS Configuration

**Always validate TLS settings:**
- Certificate chain handling (certificate + optional intermediates)
- Cipher suite and protocol allow-lists and deny-lists
- Client authentication modes: `NONE`, `REQUESTED`, `REQUIRED`
- Trust store configuration for custom CAs

## Sensitive Configuration

**Use PasswordProvider for secrets:**
- Inline passwords (for development only)
- File-based passwords (production)
- Never hardcode secrets in examples

**Example documentation pattern:**
```yaml
# ✅ Show generating random password
password: $(openssl rand -base64 32)

# ❌ Don't show hardcoded dummy value
password: "changeme"
```

## Subject and Principal Validation

**Subject composition rules:**
- Non-anonymous subjects must have exactly one `User` principal
- Use `@Unique` annotation on custom principal types
- Validate principal presence before authorisation decisions

## Resource Type Validation

**Prevent silent failures:**
- Resource type validation prevents silent failures where policies cannot be enforced
- Validate that authorisation policies can actually be applied to requested resource types

## Threat Model Awareness

**Code defensively against:**
- Malicious clients (untrusted input)
- Compromised proxy instances (limit blast radius)
- Network attackers (require TLS)
- Malevolent Kafka cluster admin (client-side controls)
- Plugin developers who might violate contracts (enforce via API design)

## Audit Logging

**Log security-relevant events:**
- Authentication success/failure
- Authorisation decisions (especially denials)
- Key rotation events
- Configuration changes affecting security

**Don't log:**
- Sensitive key material
- Full request/response bodies (may contain secrets)
- Passwords or credentials
