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
- **ServiceAccount**: Pod identity for RBAC

**Optional resources:**
- **Ingress/Route**: External access (if `KafkaProxyIngress` exists)
- **PodDisruptionBudget**: High availability (if specified)
- **NetworkPolicy**: Network isolation (if specified)

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

The operator has three layers of tests, each with a distinct scope.

**Unit tests** test one reconciler's logic in isolation — given this input resource, does the reconciler produce the right Kubernetes output? They run without a real cluster, using Mockito or the fabric8 mock client to stub API calls.

**Integration tests** (`*IT` classes) run against a real cluster and test that the reconciler interacts with Kubernetes correctly over time: does it set the right status conditions? does it react to resource changes? They use a three-actor model to keep test intent explicit:

- The **operator under test** is managed by `LocalKroxyliciousOperatorExtension`, which handles RBAC, namespace, CRD installation, and cleanup.
- The **cluster user** (`ClusterUser`) represents an end user — it creates, modifies, and deletes custom resources. Code that goes through `ClusterUser` is a user action.
- The **external operator** (`ExternalOperator`) drives status state that a reconciler outside the one under test would normally produce — either a sibling Kroxylicious reconciler or a truly external controller such as Strimzi. This ensures that dependent preconditions are exactly what the test needs without relying on other reconcilers to run.

Most ITs register only the single reconciler under test, using `ExternalOperator` to supply any dependent state produced by sibling reconcilers. `AllReconcilersIT` registers all reconcilers together and focuses on end-to-end status conditions — verifying that the reconcilers work correctly as a whole rather than testing each in depth.

See [kroxylicious-operator-test-support/README.md](../kroxylicious-operator-test-support/README.md) for the module's API and setup patterns.

**System tests** are end-to-end tests that run against a real Kafka cluster deployed via the operator. They live in a separate system-test module and verify full proxy behaviour — producing and consuming through a proxied cluster — rather than operator reconciliation logic in isolation.

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
