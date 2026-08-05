# SCRAM Credential Store API

Pluggable API for SCRAM credential storage used by SASL termination filters.

This module defines the API for credential stores that provide SCRAM credentials for SASL authentication.
It provides an abstraction for calling code to authenticate clients against various backing stores without depending on specific implementations.
In practice the calling code is expected to be some kind of SASL terminating protocol filter, such as the `SaslTermination` filter.

## Implementing a Credential Store

1. Create a configuration class
2. Implement `ScramCredentialStoreService<YourConfig>`
3. Implement `ScramCredentialStore`
4. Register via Java ServiceLoader in `META-INF/services/io.kroxylicious.scram.credentialstore.ScramCredentialStoreService`

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
        └── io.kroxylicious.scram.credentialstore.ScramCredentialStoreService
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

## See Also

- [KMS API](../kroxylicious-kms/) - Similar pluggable service pattern
