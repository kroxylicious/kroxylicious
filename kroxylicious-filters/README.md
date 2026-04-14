# kroxylicious-filters Module

This parent module contains end-user-facing filter implementations. See also [`../README.md`](../README.md) for project-wide context.

## Module Purpose

Filters in this module are intended for production use by end users. They demonstrate filter patterns and serve as reference implementations for plugin developers.

## Dependency Restrictions

**Critical constraint:** Filter modules under `kroxylicious-filters/` must **only depend on public APIs**:

- ✅ `kroxylicious-api`
- ✅ `kroxylicious-kms` (if implementing encryption)
- ✅ `kroxylicious-authorizer-api` (if implementing authorization)
- ✅ Standard Java libraries
- ❌ `kroxylicious-runtime` - **prohibited**
- ❌ `kroxylicious-integration-test-support` - test scope only

**Why:** This enforces API stability and ensures filters work as genuine external plugins would.

**Enforcement:** The build system validates these constraints using Maven Enforcer Plugin.

## Structuring a New Filter Module

**Directory layout:**
```
kroxylicious-filters/
└── my-new-filter/
    ├── pom.xml
    ├── src/
    │   ├── main/
    │   │   ├── java/
    │   │   │   └── io/kroxylicious/filter/myfilter/
    │   │   │       ├── MyFilter.java
    │   │   │       ├── MyFilterFactory.java
    │   │   │       └── MyFilterConfig.java
    │   │   └── resources/
    │   │       └── META-INF/services/
    │   │           └── io.kroxylicious.proxy.filter.FilterFactory
    │   └── test/
    │       └── java/
    │           └── io/kroxylicious/filter/myfilter/
    │               ├── MyFilterTest.java
    │               └── MyFilterIT.java
    └── README.md
```

**Required files:**

1. **Filter implementation** implementing one or more filter interfaces (e.g., `ProduceRequestFilter`)
2. **FilterFactory** implementing `FilterFactory<MyFilterConfig, MyFilter>`
3. **Config class** (record or class) for YAML configuration
4. **ServiceLoader registration** in `META-INF/services/io.kroxylicious.proxy.filter.FilterFactory`
5. **README.md** explaining what the filter does and how to configure it
6. **Tests** - both unit tests and integration tests

**POM dependencies:**

```xml
<dependencies>
    <dependency>
        <groupId>io.kroxylicious</groupId>
        <artifactId>kroxylicious-api</artifactId>
    </dependency>
    <!-- Test dependencies -->
    <dependency>
        <groupId>io.kroxylicious</groupId>
        <artifactId>kroxylicious-filter-test-support</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Common Filter Patterns

### Record Transformation

**Pattern:** Modify record values/keys in Produce requests and Fetch responses.

**Example:** `kroxylicious-simple-transform`, `kroxylicious-record-encryption`

**Key points:**
- Must handle both Produce (encrypt/validate) and Fetch (decrypt/transform)
- Preserve record batch structure and compression
- Handle null records gracefully

### Validation

**Pattern:** Validate requests against schemas or rules, reject invalid requests.

**Example:** `kroxylicious-record-validation`

**Key points:**
- Use `shortCircuitResponse()` to reject invalid requests
- Return clear error messages to clients
- Consider API version compatibility

### Authorization

**Pattern:** Check client permissions, allow or deny operations.

**Example:** `kroxylicious-authorization`

**Key points:**
- Fail-closed: deny by default
- Must handle all relevant request types
- Integrate with audit logging

### Metadata Transformation

**Pattern:** Modify cluster metadata seen by clients.

**Example:** `kroxylicious-multitenant`, `kroxylicious-entity-isolation`

**Key points:**
- Handle Metadata requests and responses
- Coordinate with other message types (Produce, Fetch)
- Maintain consistent view across filter chain

## Testing Strategies

### Unit Tests

Use `kroxylicious-filter-test-support` for isolated filter testing:

```java
@Test
void testFilterLogic() {
    var config = new MyFilterConfig(...);
    var factory = new MyFilterFactory();
    var filter = factory.createFilter(factoryContext, config);

    var request = new ProduceRequestData()...;
    var result = filter.onProduceRequest(request, context).toCompletableFuture().join();

    assertThat(result).isInstanceOf(ForwardResult.class);
}
```

### Integration Tests

Use `kroxylicious-integration-test-support` for tests with real Kafka clusters:

**Pattern:**
```java
@IntegrationTest
class MyFilterIT {
    @RegisterExtension
    static KafkaCluster cluster = new KafkaCluster(...);

    @RegisterExtension
    static KafkaProxy proxy = new KafkaProxy(cluster)
            .addFilter(MyFilterFactory.class, config);

    @Test
    void testEndToEnd() {
        // Create Kafka client pointing at proxy
        // Produce/consume messages
        // Verify filter behavior
    }
}
```

See `MEMORY.md` for audit logging test patterns and JSON schema validation.

## Integration with Audit Logging

Filters should emit audit events for significant actions:

```java
context.auditLogger().log(
    AuditAction.of("MyFilterAction"),
    objectRef,
    Map.of("detail", "value")
);
```

**Guidelines:**
- Log security-relevant decisions (authorization, authentication)
- Log data transformations (encryption, masking)
- Don't log on every message (too verbose)
- Use structured data for audit details

See `kroxylicious-api/src/main/resources/schemas/audit_action_v1.json` for audit event schema.

## Integration with Metrics

Filters can emit custom metrics:

```java
Counter counter = context.metrics().counter("my_filter_action_total",
    "description",
    "labelKey", "labelValue");
counter.increment();
```

**Guidelines:**
- Use `_total` suffix for counters
- Use clear metric names and descriptions
- Add relevant labels (action type, outcome, etc.)
- Document metrics in filter README

## Cross-References

- **Filter API**: See [`../kroxylicious-api/README.md`](../kroxylicious-api/README.md)
- **Protocol contracts**: See [`../README.md#architecture`](../README.md#architecture)
- **Testing guidance**: See [`../README.md#testing`](../README.md#testing)
- **Audit system**: See `MEMORY.md` for audit architecture and patterns
