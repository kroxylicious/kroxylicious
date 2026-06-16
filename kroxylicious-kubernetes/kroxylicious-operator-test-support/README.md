# Kroxylicious Operator Test Support

JUnit Jupiter extensions and utilities that implement the three-actor testing model for Kroxylicious operator integration tests. See the [operator README](../kroxylicious-operator/README.md) for the philosophy behind this model.

## Setting up an IT

Declare `LocalKroxyliciousOperatorExtension` as a static field and register the reconcilers under test. Obtain `ClusterUser` and `ExternalOperator` in `@BeforeEach`.

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
}
```

The `@EnabledIf` guard causes tests to skip gracefully when no cluster is reachable. `OperatorTestUtils.isKubeClientAvailable()` probes with a short timeout so it fails fast.

## Third-party CRDs

If the reconciler under test watches a third-party CRD (e.g. Strimzi `Kafka`), install it via `withSetupAction` / `withTeardownAction` and register the CR type for between-test cleanup:

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

- **Per test class** (`@BeforeAll` / `@AfterAll`): RBAC, namespace creation, CRD installation, operator startup, and final namespace teardown. The namespace and CRDs are shared across all tests in the class to avoid the cost of per-test CRD churn.
- **Per test** (`@AfterEach`): all Kroxylicious CRs, Secrets, and ConfigMaps in the namespace are deleted. Additional types registered via `withAdditionalCleanupTypes` are also deleted.

## What belongs in each actor

| What you're doing | Use |
|---|---|
| Creating or modifying a custom resource | `ClusterUser` |
| Reading a CR to assert on its status | `ClusterUser` |
| Simulating Strimzi or another external controller setting resource status | `ExternalOperator` |
| Simulating a sibling Kroxylicious reconciler setting status on a resource the reconciler-under-test depends on | `ExternalOperator` |
| Reading operator-managed resources (Deployments, ConfigMaps written by the reconciler) | `ClusterUser` |
