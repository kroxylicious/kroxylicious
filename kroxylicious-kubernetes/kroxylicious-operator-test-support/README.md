# Kroxylicious Operator Test Support

JUnit Jupiter extensions and utilities for writing Kroxylicious operator integration tests. They implement a three-actor model in which the **operator under test** runs under `LocalKroxyliciousOperatorExtension`, a **cluster user** (`ClusterUser`) performs user-side CR interactions, and an **external operator** (`ExternalOperator`) drives status state that sibling or external controllers would normally produce. This separation makes each test readable as a self-contained story of who did what and what the operator produced in response.

See the [operator README](../kroxylicious-operator/README.md) for the testing philosophy, including when to use each test layer and why the actor model exists.

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

We treat tests as executable specifications — each test tells a coherent story: a user did X, an external system did Y, and the operator-under-test produced Z. The three-actor model exists to make that story legible to test *readers*; it is a convention, not a technical enforcement.

The `operator` extension field has Kubernetes access internally (it needs it to manage the test lifecycle), but using it directly in test bodies breaks the story. A reader who sees `operator.someKubeCall()` cannot tell whether that represents a user action, an external controller, or internal test plumbing. Code that goes through the cluster user (`ClusterUser`) is a user action; code that goes through the external operator (`ExternalOperator`) is an external controller; everything else disappears into the background.

| What you're doing | Use |
|---|---|
| Creating or modifying a custom resource | `ClusterUser` |
| Reading a CR to assert on its status conditions | `ClusterUser` |
| Reading operator-managed resources (e.g. the proxy `ConfigMap` to verify virtual cluster config) | `ClusterUser` |
| Simulating Strimzi or another external controller setting resource status | `ExternalOperator` |
| Simulating a sibling Kroxylicious reconciler setting status on a resource the reconciler-under-test depends on | `ExternalOperator` |
