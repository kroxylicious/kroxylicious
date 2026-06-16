# Kroxylicious Operator Test Support

JUnit Jupiter extensions and utilities for writing integration tests against the Kroxylicious operator.

## Testing philosophy

Operator integration tests involve three distinct actors, each with a different role:

| Actor | Class | Role |
|-------|-------|------|
| **Operator under test** | `LocalKroxyliciousOperatorExtension` | The system being tested. Manages the full reconciler lifecycle: RBAC, namespace, CRDs, operator start/stop, and between-test cleanup. |
| **Cluster user** | `ClusterUser` | Represents an end user interacting with the cluster — creating, updating, and deleting Kroxylicious custom resources. Scoped to the RBAC of a regular user, not the operator itself. |
| **External operator** | `ExternalOperator` | Drives status state that a reconciler external to the one under test would normally produce. "External" includes both truly external controllers (e.g. Strimzi setting status on a `Kafka` resource) and sibling Kroxylicious reconcilers whose output the reconciler-under-test depends on. |

Keeping these actors separate makes the intent of each test action legible: code that goes through `ClusterUser` is a user action; code that goes through `ExternalOperator` is a precondition being established by another controller.

## Setting up an IT

### Single-reconciler tests

Register only the reconciler under test. Use `ExternalOperator` to supply the status state that sibling reconcilers would normally produce.

```java
@EnabledIf(value = "io.kroxylicious.testing.operator.OperatorTestUtils#isKubeClientAvailable",
           disabledReason = "no viable kube client available")
class KafkaProxyReconcilerIT {

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new KafkaProxyReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR))
            .build();

    private ClusterUser clusterUser;
    private ExternalOperator externalOperator;

    @BeforeEach
    void setUp() {
        clusterUser = operator.clusterUser();
        externalOperator = operator.externalOperator();
    }

    @Test
    void shouldSetReadyConditionOnValidProxy() {
        // Given
        KafkaProxy proxy = clusterUser.create(new KafkaProxyBuilder()
                .withNewMetadata().withName("my-proxy").endMetadata()
                .build());

        // When / Then — reconciler sets the Ready condition
        AWAIT.untilAsserted(() ->
            assertThat(clusterUser.get(KafkaProxy.class, "my-proxy"))
                .isNotNull()
                .extracting(KafkaProxy::getStatus)
                // ... assert conditions
        );
    }
}
```

### Multi-reconciler tests

To test reconcilers working together, register all of them. The `AllReconcilersIT` is the canonical example: it focuses on status conditions reported end-to-end, while the individual reconciler ITs test each reconciler's logic in depth.

### Third-party CRDs (e.g. Strimzi)

If the reconciler under test watches a third-party CRD, install it via `withSetupAction` / `withTeardownAction` and register the CR type for between-test cleanup with `withAdditionalCleanupTypes`:

```java
@RegisterExtension
static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
        .withReconciler(new KafkaServiceReconciler(Clock.systemUTC(), sharedInformerManager))
        .withSetupAction(() -> {
            try (KubernetesClient client = OperatorTestUtils.kubeClient()) {
                client.apiextensions().v1().customResourceDefinitions()
                      .resource(Crds.kafka()).createOr(Updatable::update);
            }
        })
        .withTeardownAction(() -> {
            try (KubernetesClient client = OperatorTestUtils.kubeClient()) {
                client.apiextensions().v1().customResourceDefinitions()
                      .resource(Crds.kafka()).delete();
            }
        })
        .withAdditionalCleanupTypes(Kafka.class)
        .build();
```

## Lifecycle

`LocalKroxyliciousOperatorExtension` manages state at two granularities:

- **Per test class** (`@BeforeAll` / `@AfterAll`): RBAC setup, namespace creation, CRD application, operator startup, and final cleanup. The namespace and CRDs are shared across all tests in the class to avoid the overhead of per-test CRD deletion and re-application.
- **Per test** (`@AfterEach`): all Kroxylicious CRs, Secrets, and ConfigMaps in the namespace are deleted between tests. Additional resource types registered via `withAdditionalCleanupTypes` are also deleted.

Obtain fresh `ClusterUser` and `ExternalOperator` instances in `@BeforeEach` — they are lightweight wrappers and safe to recreate each test.

## Skipping when no cluster is available

Guard every IT class with `@EnabledIf` so tests skip gracefully on machines without a running Kubernetes cluster:

```java
@EnabledIf(
    value = "io.kroxylicious.testing.operator.OperatorTestUtils#isKubeClientAvailable",
    disabledReason = "no viable kube client available"
)
```

`OperatorTestUtils.isKubeClientAvailable()` probes the cluster with a short timeout so it fails fast rather than hanging.
