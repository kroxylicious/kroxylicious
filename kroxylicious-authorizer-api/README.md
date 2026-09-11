# kroxylicious-authorizer-api Module

This module defines the Authorization plugin API.
See also [`../README.md`](../README.md) for project-wide context.

## API Roles

**Authorizer API Users/Callers**: Filters that call `Authorizer` methods to make access control decisions over collections of entities.
`AuthorizationFilter` (in `kroxylicious-filters/kroxylicious-authorization`) is the primary caller, but this is not limited to that filter.

**Authorizer Implementers**: Developers writing `Authorizer` implementations that integrate with policy engines (ACL systems, OPA, etc.).

## Dependencies

This module depends only on `kroxylicious-identity-api` (a lightweight, zero-dependency module) and small internal utility packages.
It deliberately does **not** depend on `kroxylicious-api`, so it does not transitively pull in `kafka-clients`, `jackson-annotations` or compression codec libraries.
This makes it practical for non-Kroxylicious projects to implement or consume `Authorizer`.

---

# For Authorizer API Users (Filters)

This section describes how to use the Authorizer API when implementing filters that need authorization.

## API Overview

The Authorizer API provides authorization decisions for Kafka-like resources and actions.
It decouples authorization logic from filter implementation.
Its asynchronous return type supports both in-process and networked policy decision points (e.g. OPA, OpenFGA).

**Core interface:** `Authorizer`

**Key method you call:**

```java
CompletionStage<AuthorizeResult> authorize(Identity subject, List<Action> actions);
```

`Identity` (from `kroxylicious-identity-api`) is a deprecated-at-birth bridge interface implemented by both `io.kroxylicious.identity.Subject` and the deprecated `io.kroxylicious.proxy.authentication.Subject`.
In practice you obtain it from `FilterContext.authenticatedSubject()`, which currently returns the latter — no conversion is needed, it already implements `Identity`.

**Also available:**

```java
Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes();
```

An empty `Optional` means the authorizer's supported resource types are not known.
A filter should treat that permissively rather than rejecting all actions.

## Calling the API

**Basic pattern** (see `AuthorizationFilter` for the real implementation):

```java
Identity subject = context.authenticatedSubject();
List<Action> actions = List.of(new Action(Topic.WRITE, "my-topic"));

CompletionStage<AuthorizeResult> result = authorizer.authorize(subject, actions);
```

**`Action`:**

```java
record Action(ResourceType<?> operation, String resourceName) { }
```

`operation` identifies both the operation and the resource type (see `ResourceType` below).
`resourceName` names the concrete resource.

**`AuthorizeResult`:**

```java
record AuthorizeResult(
    Identity subject,
    List<Action> allowed,
    List<Action> denied
) { }
```

- **`allowed`**: actions the subject is permitted to perform
- **`denied`**: actions the subject is not permitted to perform
- **Guarantee**: the implementation must partition all of the requested `actions` between `allowed` and `denied`

`AuthorizeResult` also has convenience methods: `allowed(ResourceType<?>)` / `denied(ResourceType<?>)` (resource names for a given operation), `decision(ResourceType<?>, String)` (the `Decision` for a single resource), and `partition(Collection<T>, ResourceType<?>, Function<T, String>)` (partitions an arbitrary collection of items by decision).

**Using the result:**

```java
result.thenApply(authzResult -> {
    if (authzResult.decision(Topic.WRITE, "my-topic") == Decision.ALLOW) {
        return context.forwardRequest(header, request);
    }
    else {
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(unauthorizedError())
                .completed();
    }
});
```

## Validating Resource Type Support

**At filter initialization**, check that the authorizer supports the resource types you need:

```java
Optional<Set<Class<? extends ResourceType<?>>>> supported = authorizer.supportedResourceTypes();
if (supported.isPresent() && !supported.get().contains(Topic.class)) {
    throw new ConfigurationException("Authorizer doesn't support the Topic resource type");
}
```

This prevents silent failures where policies cannot be enforced.

## Handling Anonymous Subjects

An `Identity` with no principals is anonymous:

```java
boolean isAnonymous = subject.isAnonymous();
```

Whether an anonymous subject is allowed or denied is a decision made by the `Authorizer` implementation and its configured policy, not by the caller.

## Caching (Optional)

You may cache authorization decisions to reduce latency, but use a TTL and never cache "allow" decisions indefinitely, since permissions may be revoked.

---

# For Authorizer Implementers

This section describes the requirements when implementing an `Authorizer`.

## Implementation Contract

**Methods you must implement:**

```java
CompletionStage<AuthorizeResult> authorize(Identity subject, List<Action> actions);

Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes();
```

**Requirements:**

- **Async, non-blocking**: must return `CompletionStage` and never block event loop threads
- **Batch authorization**: a single call must authorize multiple actions
- **Fail-closed**: on error, deny by default; the returned stage should fail with `AuthorizerException` if a decision genuinely cannot be made
- **Partitioning**: every requested action must end up in exactly one of `allowed` or `denied`

## Fail-Closed Implementation

**Default-deny principle:** you must deny by default.
When in doubt, deny (see [security-patterns.md](../.claude/rules/security-patterns.md)).

**Error handling pattern:**

```java
@Override
public CompletionStage<AuthorizeResult> authorize(Identity subject, List<Action> actions) {
    return policyEngine.evaluateAsync(subject, actions)
            .handle((result, error) -> {
                if (error != null) {
                    // On error, deny all actions
                    return new AuthorizeResult(subject, List.of(), actions);
                }
                return result;
            });
}
```

**Never:**
- Allow actions when policy evaluation fails
- Cache "allow" decisions indefinitely (use a TTL)

## Resource Type Declaration

**You must declare** which resource types your implementation supports, or return `Optional.empty()` if this is not statically known (for example, an authorizer backed by an external policy engine that can be reconfigured with new rules at runtime):

```java
@Override
public Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes() {
    return Optional.of(Set.of(Topic.class, ConsumerGroup.class));
}
```

`ResourceType<S>` is implemented as an enum of the operations supported on a given resource kind — one enum per resource kind, so the enum's `Class` also identifies the resource type.
Callers use `supportedResourceTypes()` to validate that you support the resource types they need.

## Working with Identity and Principals

**Extract information from an `Identity`:**

```java
Set<? extends Principal> principals = subject.principals();
Optional<User> user = subject.uniquePrincipalOfType(User.class); // for @SingularPrincipal-annotated types
Set<CustomPrincipal> roles = subject.allPrincipalsOfType(CustomPrincipal.class);
```

`uniquePrincipalOfType` throws `IllegalArgumentException` if the given type is not annotated `@SingularPrincipal`, or, via the deprecated bridge, `@Unique`.

**Handle anonymous subjects:**

```java
if (subject.isAnonymous()) {
    return CompletableFuture.completedStage(new AuthorizeResult(subject, List.of(), actions));
}
```

## Async Implementation Patterns

**Requirement**: you must not block event loop threads.

**Pattern for an external policy engine:**

```java
@Override
public CompletionStage<AuthorizeResult> authorize(Identity subject, List<Action> actions) {
    return httpClient.postAsync("/authorize", toJson(subject, actions))
            .thenApply(response -> parseAuthzResult(subject, response))
            .exceptionally(error -> {
                logger.error("Authorization failed", error);
                // Fail-closed: deny all on error
                return new AuthorizeResult(subject, List.of(), actions);
            });
}
```

## Performance Considerations for Implementations

**Caching:** you should cache authorization decisions to reduce latency (but use a TTL).

**Connection pooling**: reuse connections to external policy engines.

## Security Requirements for Implementations

**Policy evaluation:**

- Your policies must be deterministic (same inputs → same output)
- Avoid time-dependent policies (hard to test, audit)
- Log all deny decisions for audit trail

**Policy integrity:**

- Policies from external files must be integrity-checked
- Validate policy syntax at initialization, not at authorization time
- Detect and reject policy tampering

**Threat model:**

- **Malicious clients**: will attempt to bypass authorization (your fail-closed implementation prevents this)
- **Policy tampering**: validate policy file integrity
- **Compromised authorizer**: defence-in-depth via audit logging helps detect this

## Testing Your Implementation

**Unit tests:**

```java
@Test
void testAllowedAction() {
    Identity subject = new io.kroxylicious.identity.Subject(Set.of(new User("alice")));
    var actions = List.of(new Action(Topic.WRITE, "test-topic"));

    var result = authorizer.authorize(subject, actions).toCompletableFuture().join();

    assertThat(result.allowed()).containsExactlyElementsOf(actions);
    assertThat(result.denied()).isEmpty();
}

@Test
void testFailClosed() {
    var authorizer = new FaultyAuthorizer(); // Throws exceptions
    Identity subject = new io.kroxylicious.identity.Subject(Set.of(new User("alice")));
    var actions = List.of(new Action(Topic.READ, "any-topic"));

    var result = authorizer.authorize(subject, actions).toCompletableFuture().join();

    // Must deny all on error
    assertThat(result.allowed()).isEmpty();
    assertThat(result.denied()).isEqualTo(actions);
}
```

**Integration tests:**

Test with `AuthorizationFilter` and real Kafka clusters.
Verify both allowed and denied actions produce the expected client-visible behaviour (successful requests vs. authorization errors).

## Registration

Authorizers are built by an `AuthorizerService<C>`, not registered directly:

```java
public interface AuthorizerService<C> {
    void initialize(C config);
    Authorizer build();
    default void close() { }
}
```

Your service implementation must:
- Implement `AuthorizerService<C>`, annotated with `@Plugin(configType = C.class)`
- Be registered via `ServiceLoader`, in `META-INF/services/io.kroxylicious.authorizer.service.AuthorizerService`
- Include tests demonstrating allow/deny decisions and fail-closed behavior

See `AclAuthorizerService` for a working example.

---

# Reference Implementations

**Included authorizer implementations:**

- **`AclAuthorizer`** (`kroxylicious-authorizer-providers/kroxylicious-authorizer-acl`): ACL-based authorization with rules built programmatically, or parsed from a file using a naturalish-language grammar.

Study this implementation for patterns and best practices.

## Cross-References

- **Security model**: See [`../README.md#security-model`](../README.md#security-model)
- **Identity types**: See [`../kroxylicious-identity-api/README.md`](../kroxylicious-identity-api/README.md)
- **Filter API**: See [`../kroxylicious-api/README.md`](../kroxylicious-api/README.md)
- **Authorization filter**: See `../kroxylicious-filters/kroxylicious-authorization/`
