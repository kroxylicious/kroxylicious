# kroxylicious-operator Module

This module implements the Kubernetes operator for Kroxylicious. See also [`../README.md`](../README.md) for project-wide context.

## Operator Purpose

The operator automates deployment and management of Kroxylicious proxy instances on Kubernetes. It observes Custom Resources (CRs) and reconciles actual cluster state to match desired state.

## Custom Resources

**Primary CRs:**

- **`KafkaProxy`**: Defines a proxy deployment (replicas, configuration, filters)
- **`KafkaProxyIngress`**: Defines how clients access the proxy (load balancer, routes)
- **`KafkaProtocolFilter`**: Defines filter configurations (referenced by `KafkaProxy`)

**CRD Module:**

Custom Resource Definitions live in `kroxylicious-kubernetes-api` module. The Java classes in that module are generated from the CRD schemas, but the **CRD YAML schemas themselves** are a public API and comes with the usual Kubernetes compatibility guarantee for custom resources.

## Operator's public API

The environment variables, command line arguments and configuration files for the operator itself are also a public API which comes with compatibility guarantees.

## Reconciliation Patterns

The operator is implemented using the Java Operator SDK, which defines most of the patterns being used.

**Reconciliation loop:**

The operator watches CRs and reconciles them:

1. **Observe**: Watch for CR create/update/delete events
2. **Analyse**: Compare desired state (CR spec) to actual state (Kubernetes resources)
3. **Reconcile**: Create/update/delete Kubernetes resources to match desired state
4. **Update status**: Write reconciliation result to CR status field

**Periodic reconciliation:**

The operator relies on event-driven reconciliation: each custom resource is reconciled in response to Kubernetes events. Out of the box, the operator does not override the JOSDK maximum reconciliation interval, so the JOSDK default (10 hours) applies.

A shorter periodic reconciliation interval can be enabled as an escape hatch by setting the `KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS` environment variable in the operator deployment. When set, each custom resource is independently reconciled after the interval expires, even when no Kubernetes events occur, which helps detect and correct drift caused by external modifications or transient reconciliation failures. Event-driven reconciliation remains the primary mechanism and triggers immediately.

Note: Periodic reconciliation reads from the informer cache, not directly from the API server. If watch connections fail, the cache may become stale until the watch reconnects and re-synchronizes.

For example, to enable a 5-minute interval:

```yaml
env:
  - name: KROXYLICIOUS_OPERATOR_RESYNC_INTERVAL_SECONDS
    value: "300"
```

**Pattern:**

```java
@Override
public UpdateControl<KafkaProxy> reconcile(
        KafkaProxy kafkaProxy,
        Context<KafkaProxy> context) {
    // 1. Read desired state from CR
    var desiredReplicas = kafkaProxy.getSpec().getReplicas();
    var desiredConfig = buildProxyConfig(kafkaProxy);

    // 2. Get actual state
    var deployment = client.apps().deployments()
            .inNamespace(namespace)
            .withName(deploymentName(kafkaProxy))
            .get();

    // 3. Reconcile
    if (deployment == null) {
        createDeployment(kafkaProxy, desiredConfig);
    } else if (!matches(deployment, desiredReplicas, desiredConfig)) {
        updateDeployment(deployment, desiredReplicas, desiredConfig);
    }

    // 4. Update status
    kafkaProxy.getStatus().setReady(true);
    return UpdateControl.updateStatus(kafkaProxy);
}
```

**Idempotency:**

Reconciliation must be idempotent: calling it multiple times with the same input produces the same result. Don't assume reconciliation runs exactly once.

**Error handling:**

Reconciliation errors trigger automatic retry (with exponential backoff):

```java
try {
    reconcile(kafkaProxy);
    return UpdateControl.updateStatus(kafkaProxy);
} catch (Exception e) {
    logger.error("Reconciliation failed", e);
    kafkaProxy.getStatus().setError(e.getMessage());
    return UpdateControl.updateStatus(kafkaProxy)
            .rescheduleAfter(Duration.ofMinutes(1));
}
```

**Event source mapping:**

Each reconciler's `prepareEventSources` registers, for every related resource type, an event source built via `io.kroxylicious.kubernetes.operator.eventsource.EventSourceBuilder`, declaring a `PrimaryToSecondaryMapper` (given the primary, find the secondaries it should watch) and a `SecondaryToPrimaryMapper` (given a changed secondary, find the primaries to reconcile). Every declarative relationship, regardless of which side holds the reference, is expressed via the `io.kroxylicious.kubernetes.operator.eventsource.Ref` DSL and passed to `EventSourceBuilder.withRelationship(...)`:

- `Ref.toSecondary`/`Ref.toSecondaries` — the primary's spec holds a `LocalRef` (or collection of them) pointing at its secondaries, e.g. `VirtualKafkaCluster.spec.targetKafkaServiceRef`. The primary→secondary direction is a direct O(1) lookup; the secondary→primary direction scans the primary cache for matches.
- `Ref.toPrimary`/`Ref.toPrimaryVia` — the secondary (perhaps transitively, through an intermediate resource) holds the reference back to the primary, e.g. `KafkaProxyIngress.spec.proxyRef`, or `KafkaProtocolFilter` reached via `VirtualKafkaCluster.spec.filterRefs`. Because an update/delete event only exposes the secondary's current state, the secondary→primary direction conservatively reconciles every primary in the namespace rather than risk missing a reference removal (see [#4017](https://github.com/kroxylicious/kroxylicious/issues/4017) for the related live-API-call pitfall this also avoids).

`EventSourceBuilder.withSharedInformer(...)` opts into a cache shared across reconcilers watching the same secondary type. The builder automatically drops events from Kroxylicious CR secondaries whose status hasn't caught up with their spec (those for which `ResourcesUtil.hasObservedGeneration(type)` is true); core Kubernetes types have no generation/observedGeneration semantics and are never guarded. Call `.withoutFreshStatus()` to opt a status-bearing secondary out of the guard, as the `KafkaProxy` sources do so that a proxy change always reconciles its dependents regardless of the proxy's own reconcile state.

A relationship still needs a bespoke mapper pair, plugged in via `EventSourceBuilder.withMappers(...)`, when it doesn't reduce to a same-namespace `LocalRef` lookup:
- **Cross-namespace references** — resolved via a `context.getPrimaryCache().addIndexer(...)`/`byIndex(...)` lookup instead of a same-namespace scan (e.g. `StrimziKafkaSecondaryToKafkaServicePrimaryMapper`, since a `KafkaService`'s `strimziKafkaRef` can point at a `Kafka` resource in a different namespace).
- **Owner-reference / derived-name resolution** for operator-owned resources, where the secondary isn't referenced by a `spec` ref field at all (e.g. `KubernetesServicesSecondaryToVirtualKafkaClusterPrimaryMapper`, which correlates owned `Service` objects via Kubernetes `ownerReferences` and conventional naming).
- **Dynamic, interpolation-driven discovery**, where the references aren't declarative spec fields at all (e.g. `KafkaProtocolFilterReconciler` extracts Secret/ConfigMap names from its interpolated `spec.configTemplate`).

## CRD to Proxy Configuration Mapping

**Transformation:**

The operator transforms CRs into proxy YAML configuration:

1. **KafkaProxy CR** → Proxy deployment with ConfigMap containing YAML config
2. **KafkaProtocolFilter CRs** → Filter configurations in the YAML
3. **KafkaProxyIngress CR** → Service and Ingress/Route resources

**Example mapping:**

```yaml
# KafkaProxy CR
apiVersion: kroxylicious.io/v1alpha1
kind: KafkaProxy
metadata:
  name: my-proxy
spec:
  replicas: 3
  filters:
    - filterRef:
        name: my-filter
  upstreamCluster:
    bootstrapServers: kafka:9092
```

Becomes:

```yaml
# Proxy ConfigMap with YAML config
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-proxy-config
data:
  config.yaml: |
    filters:
      - type: io.kroxylicious.filter.MyFilter
        config: {...}
    clusters:
      kafka:
        bootstrap: kafka:9092
```

**Config validation:**

Validate configurations during reconciliation (fail early):

```java
try {
    var config = buildProxyConfig(kafkaProxy);
    validateConfig(config); // Throws if invalid
} catch (ConfigException e) {
    kafkaProxy.getStatus().setError("Invalid configuration: " + e.getMessage());
    return UpdateControl.updateStatus(kafkaProxy);
}
```

## Deployment Resources Created

For each `KafkaProxy` CR, the operator creates:

**Core resources:**
- **Deployment**: Runs proxy pods (replicas specified in CR)
- **ConfigMap**: Contains proxy YAML configuration
- **Service**: Exposes proxy pods (ClusterIP or LoadBalancer)

**Optional resources:**
- **Route**: External access on OpenShift clusters (when `openShiftRoute` is supported and configured)

> **Note:** The operator does not create a `ServiceAccount`, `NetworkPolicy`, `PodDisruptionBudget`, or Kubernetes `Ingress`.

**Ownership:**

Resources must be owned by the CR (for garbage collection):

```java
deployment.getMetadata().setOwnerReferences(List.of(
    new OwnerReferenceBuilder()
        .withApiVersion(kafkaProxy.getApiVersion())
        .withKind(kafkaProxy.getKind())
        .withName(kafkaProxy.getMetadata().getName())
        .withUid(kafkaProxy.getMetadata().getUid())
        .withController(true)
        .build()
));
```

## Operator-Specific Testing

The operator has three test layers, each documenting a wider scope of value. The technical characteristics of each layer follow from that scope — they are not the defining property.

**Unit tests** document the value a single reconciler is responsible for: given these input resources, does it produce the correct Kubernetes output? The scope is one component's logic; the infrastructure around it is stubbed with Mockito or the fabric8 mock client because it is not part of the value claim.

**Integration tests** (`*IT` classes) widen the scope to include the reconciler's interaction with a real Kubernetes cluster. The value claim now covers status propagation, resource watches, and eventual consistency — things that only manifest against a real API server. They use a three-actor model to keep test intent explicit:

- The **operator under test** is managed by `LocalKroxyliciousOperatorExtension`, which handles RBAC, namespace, CRD installation, and cleanup.
- The **cluster user** (`ClusterUser`) represents an end user — it creates, modifies, and deletes custom resources. Code that goes through `ClusterUser` is a user action.
- The **external operator** (`ExternalOperator`) drives status state that a reconciler outside the one under test would normally produce — either a sibling Kroxylicious reconciler or a truly external controller such as Strimzi. This ensures that dependent preconditions are exactly what the test needs without relying on other reconcilers to run.

Most ITs register only the single reconciler under test, using `ExternalOperator` to supply any dependent state produced by sibling reconcilers. `AllReconcilersIT` registers all reconcilers together and widens the scope further — verifying that the reconcilers deliver their combined value correctly, rather than testing each in depth.

See [kroxylicious-operator-test-support/README.md](../kroxylicious-operator-test-support/README.md) for the module's API and setup patterns.

**System tests** widen the scope to the full user-visible value: does the proxy, deployed by the operator, correctly proxy Kafka traffic end-to-end? They live in a separate system-test module and verify producing and consuming through a real proxied cluster.

## Operator Lifecycle

**Startup:**

1. Register CRD schemas (if not already present)
2. Start informers (watch CRs and resources)
3. Begin reconciliation loops

**Shutdown:**

1. Stop accepting new reconciliation requests
2. Finish in-flight reconciliations
3. Close Kubernetes client connections

**Leader election:**

For high availability, run multiple operator instances with leader election:

```java
@LeaderElection(leaseName = "kroxylicious-operator-leader")
public class OperatorMain {
    public static void main(String[] args) {
        var operator = new Operator(client);
        operator.start();
    }
}
```

## Configuration and Secrets

**TLS certificates:**

The operator must handle TLS configuration:

```yaml
spec:
  tls:
    certificateRef:
      name: proxy-cert
      namespace: default
    trustAnchorRef:
      name: ca-cert
```

Maps to:

1. Read Secret containing certificate/key
2. Mount Secret as volume in proxy pods
3. Generate proxy YAML referencing mounted paths

**Sensitive configuration:**

Never log or expose:
- TLS private keys
- Passwords
- API tokens
- KMS credentials

## Cross-References

- **CRD definitions**: See `../kroxylicious-kubernetes-api/`
- **Proxy configuration**: See [`../README.md#configuration`](../README.md#configuration)
- **Deployment model**: See [`../README.md#deployment-considerations`](../README.md#deployment-considerations)
- **Security model**: See [`../README.md#security-model`](../README.md#security-model)
