# kroxylicious-authorizer-api Module

This module defines the Authorization plugin API. See also [`../README.md`](../README.md) for project-wide context.

## API Roles

**Authorizer API Users/Callers**: Filters that call Authorizer methods to make access control decisions over collections of entities. This includes `AuthorizationFilter`, is not limited to that.

**Authorizer Implementers**: Developers writing Authorizer implementations that integrate with policy engines (ACL systems, OPA, etc.).

---

# For Authorizer API Users (Filters)

This section describes how to use the Authorizer API when implementing filters that need authorization.

## API Overview

The Authorizer API provides authorization decisions for Kafka resources and actions. It decouples authorization logic from filter implementation.

**Core interface:** `Authorizer`

**Key method you call:**

```java
CompletionStage<AuthorizeResult> authorize(
    Subject subject,
    Set<Action> actions,
    ResourceSpec resource
);
```

**Also available:**

```java
Set<ResourceType> supportedResourceTypes();
```

## Calling the API

**Basic pattern:**

```java
Subject subject = filterContext.getAuthenticatedSubject();
Set<Action> actions = Set.of(Action.WRITE, Action.READ);
ResourceSpec resource = new TopicResource("my-topic");

CompletionStage<AuthorizeResult> result =
    authorizer.authorize(subject, actions, resource);
```

**Understanding AuthorizeResult:**

```java
record AuthorizeResult(
    Set<Action> allowed,
    Set<Action> denied
) { }
```

- **`allowed`**: Actions the subject is permitted to perform
- **`denied`**: Actions the subject is explicitly denied
- **Guarantee**: `allowed ∪ denied` equals the requested `actions` set
- **Guarantee**: `allowed ∩ denied` is empty (no action in both sets)

**Using the result:**

```java
result.thenApply(authzResult -> {
    if (authzResult.allowed().contains(Action.WRITE)) {
        // Proceed with write operation
        return context.forwardRequest(request);
    } else {
        // Reject with authorization error
        return context.requestFilterResultBuilder()
                .shortCircuitResponse(unauthorizedError())
                .completed();
    }
});
```

## Validating Resource Type Support

**At filter initialization**, check that the authorizer supports the resource types you need:

```java
Set<ResourceType> supported = authorizer.supportedResourceTypes();
if (!supported.contains(ResourceType.TOPIC)) {
    throw new ConfigurationException(
        "Authorizer doesn't support TOPIC resource type"
    );
}
```

This prevents silent failures where policies cannot be enforced.

## Handling Anonymous Subjects

Subjects without a `User` principal are anonymous:

```java
boolean isAnonymous = subject.principals(User.class).findFirst().isEmpty();
if (isAnonymous) {
    // Typically deny all actions for anonymous subjects
    return authorizer.authorize(subject, actions, resource);
    // Most authorizers will deny all for anonymous subjects
}
```

## Caching (Optional)

You may cache authorization decisions to reduce latency, but use TTL:

```java
String cacheKey = cacheKey(subject, resource);
AuthorizeResult cached = cache.get(cacheKey);
if (cached != null) {
    return CompletableFuture.completedFuture(cached);
}

return authorizer.authorize(subject, actions, resource)
        .thenApply(result -> {
            cache.put(cacheKey, result, TTL);
            return result;
        });
```

**Warning**: Don't cache "allow" decisions indefinitely - permissions may be revoked.

---

# For Authorizer Implementers

This section describes the requirements when implementing an Authorizer.

## Implementation Contract

**Methods you must implement:**

```java
CompletionStage<AuthorizeResult> authorize(
    Subject subject,
    Set<Action> actions,
    ResourceSpec resource
);

Set<ResourceType> supportedResourceTypes();
```

**Requirements:**

- **Async, non-blocking**: Must return `CompletionStage` and never block event loop threads
- **Batch authorization**: Single call must authorize multiple actions on a resource
- **Fail-closed**: On error or failure, deny by default (return all actions in `denied` set)
- **Partitioning**: `allowed ∪ denied` must equal requested `actions`; `allowed ∩ denied` must be empty

## Fail-Closed Implementation

**Default-deny principle:** You must deny by default. When in doubt, deny.

**Error handling pattern:**

```java
@Override
public CompletionStage<AuthorizeResult> authorize(
        Subject subject,
        Set<Action> actions,
        ResourceSpec resource) {
    return policyEngine.evaluateAsync(subject, actions, resource)
            .handle((result, error) -> {
                if (error != null) {
                    // On error, deny all actions
                    return new AuthorizeResult(Set.of(), actions);
                }
                return result;
            });
}
```

**Never:**
- Allow actions when policy evaluation fails
- Allow actions for unauthenticated subjects (unless your policy explicitly permits)
- Cache "allow" decisions indefinitely (use TTL)

## Resource Type Declaration

**You must declare** which resource types your implementation supports:

```java
@Override
public Set<ResourceType> supportedResourceTypes() {
    return Set.of(
        ResourceType.TOPIC,
        ResourceType.TRANSACTIONAL_ID,
        ResourceType.GROUP
    );
}
```

**Available resource types:**
- `TopicResource`: Kafka topics
- `TransactionalIdResource`: Transactional IDs
- `GroupResource`: Consumer groups

Callers will validate that you support the resource types they need.

## Working with Subjects and Principals

**Extract information from Subject:**

```java
Optional<User> user = subject.principals(User.class).findFirst();
Set<CustomPrincipal> roles = subject.principals(CustomPrincipal.class)
        .collect(Collectors.toSet());

// Evaluate policy based on user and roles
```

**Handle anonymous subjects:**

```java
boolean isAnonymous = subject.principals(User.class).findFirst().isEmpty();
if (isAnonymous) {
    // Typically deny all actions for anonymous subjects
    return CompletableFuture.completedFuture(
        new AuthorizeResult(Set.of(), actions)
    );
}
```

## Async Implementation Patterns

**Requirement**: You must not block event loops.

**Pattern for external policy engine:**

```java
@Override
public CompletionStage<AuthorizeResult> authorize(
        Subject subject,
        Set<Action> actions,
        ResourceSpec resource) {
    return httpClient.postAsync("/authorize", toJson(subject, actions, resource))
            .thenApply(response -> parseAuthzResult(response))
            .exceptionally(error -> {
                logger.error("Authorization failed", error);
                // Fail-closed: deny all on error
                return new AuthorizeResult(Set.of(), actions);
            });
}
```

## Performance Considerations for Implementations

**Caching:**

You should cache authorization decisions to reduce latency (but use TTL):

```java
String cacheKey = cacheKey(subject, resource);
AuthorizeResult cached = cache.get(cacheKey);
if (cached != null) {
    return CompletableFuture.completedFuture(cached);
}

return evaluatePolicy(subject, actions, resource)
        .thenApply(result -> {
            cache.put(cacheKey, result, TTL);
            return result;
        });
```

**Connection pooling**: Reuse connections to external policy engines.

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

- **Malicious clients**: Will attempt to bypass authorization (your fail-closed implementation prevents this)
- **Policy tampering**: Validate policy file integrity
- **Compromised authorizer**: Defence-in-depth via audit logging helps detect this

## Testing Your Implementation

**Unit tests:**

```java
@Test
void testAllowedAction() {
    var subject = Subject.of(new User("alice"));
    var actions = Set.of(Action.WRITE);
    var resource = new TopicResource("test-topic");

    var result = authorizer.authorize(subject, actions, resource)
            .toCompletableFuture().join();

    assertThat(result.allowed()).contains(Action.WRITE);
    assertThat(result.denied()).isEmpty();
}

@Test
void testDeniedAction() {
    var subject = Subject.of(new User("alice"));
    var actions = Set.of(Action.DELETE);
    var resource = new TopicResource("protected-topic");

    var result = authorizer.authorize(subject, actions, resource)
            .toCompletableFuture().join();

    assertThat(result.allowed()).isEmpty();
    assertThat(result.denied()).contains(Action.DELETE);
}

@Test
void testFailClosed() {
    var authorizer = new FaultyAuthorizer(); // Throws exceptions
    var subject = Subject.of(new User("alice"));
    var actions = Set.of(Action.READ);
    var resource = new TopicResource("any-topic");

    var result = authorizer.authorize(subject, actions, resource)
            .toCompletableFuture().join();

    // Must deny all on error
    assertThat(result.allowed()).isEmpty();
    assertThat(result.denied()).isEqualTo(actions);
}
```

**Integration tests:**

Test with authorization filter and real Kafka clusters:

```java
@IntegrationTest
class AuthorizerIT {
    @RegisterExtension
    static KafkaCluster cluster = new KafkaCluster();

    @RegisterExtension
    static KafkaProxy proxy = new KafkaProxy(cluster)
            .addFilter(AuthorizationFilter.class,
                new AuthzConfig(MyAuthorizer.class, policies));

    @Test
    void testProduceAllowed() {
        // Produce to allowed topic succeeds
        producer.send(new ProducerRecord<>("allowed-topic", "value"));
    }

    @Test
    void testProduceDenied() {
        // Produce to denied topic fails with authorization error
        assertThrows(AuthorizationException.class, () ->
            producer.send(new ProducerRecord<>("denied-topic", "value")).get()
        );
    }
}
```

## Registration

Your implementation must:
- Implement `Authorizer` interface
- Provide a configuration class
- Register via `ServiceLoader` in `META-INF/services/io.kroxylicious.authorizer.Authorizer`
- Include tests demonstrating allow/deny decisions and fail-closed behavior

---

# Reference Implementations

**Included authorizer implementations:**

- **`AclAuthorizer`** (`kroxylicious-authorizer-providers/kroxylicious-authorizer-acl`): ACL-based authorization with file or embedded rules
- **`OpaAuthorizer`** (`kroxylicious-authorizer-providers/kroxylicious-authorizer-opa`): Open Policy Agent integration

Study these implementations for patterns and best practices.

## Cross-References

- **Security model**: See [`../README.md#security-model`](../README.md#security-model)
- **Filter API**: See [`../kroxylicious-api/README.md`](../kroxylicious-api/README.md)
- **Authorization filter**: See `../kroxylicious-filters/kroxylicious-authorization/`
- **Audit logging**: See `MEMORY.md` for audit architecture
