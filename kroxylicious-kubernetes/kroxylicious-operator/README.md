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

**Unit tests:**

Test reconciliation logic in isolation:

```java
@Test
void testReconcileCreatesDeployment() {
    var kafkaProxy = new KafkaProxyBuilder()
            .withNewMetadata().withName("test").endMetadata()
            .withNewSpec().withReplicas(2).endSpec()
            .build();

    var controller = new KafkaProxyController(mockClient);
    controller.reconcile(kafkaProxy, context);

    verify(mockClient.apps().deployments()).inNamespace(any())
            .create(deploymentCaptor.capture());

    var deployment = deploymentCaptor.getValue();
    assertThat(deployment.getSpec().getReplicas()).isEqualTo(2);
}
```

**Integration tests:**

Use `kroxylicious-operator-test-support` for tests with real Kubernetes:

```java
@OperatorTest
class KafkaProxyIT {
    @RegisterExtension
    static K3s k3s = new K3s();

    @Test
    void testProxyDeployment() {
        var kafkaProxy = new KafkaProxyBuilder()
                .withNewMetadata().withName("test").endMetadata()
                .withNewSpec().withReplicas(1).endSpec()
                .build();

        client.resource(kafkaProxy).create();

        // Wait for deployment to be ready
        await().untilAsserted(() -> {
            var deployment = client.apps().deployments()
                    .inNamespace(namespace)
                    .withName("test")
                    .get();
            assertThat(deployment).isNotNull();
            assertThat(deployment.getStatus().getReadyReplicas()).isEqualTo(1);
        });
    }
}
```

**System tests:**

End-to-end tests with Kafka clusters:

```java
@SystemTest
class ProxySystemTest {
    @Test
    void testProduceConsumeViaProxy() {
        // Deploy Kafka cluster
        // Deploy proxy via operator
        // Produce/consume messages through proxy
        // Verify filter behavior
    }
}
```

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
