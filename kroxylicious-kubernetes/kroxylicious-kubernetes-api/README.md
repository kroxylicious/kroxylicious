# kroxylicious-kubernetes-api Module

This module defines the Kubernetes Custom Resource Definitions (CRDs) for the Kroxylicious operator. See also [`../README.md`](../README.md) for project-wide context.

## API Roles

**CR Authors**: Kubernetes users who create and manage `KafkaProxy`, `KafkaProxyIngress`, and `KafkaProtocolFilter` custom resources.

**Operator Developers**: Kroxylicious developers working on the operator (in the `kroxylicious-operator` module) that reconciles these CRs, or extending the API with new CRDs. 3rd party developers should note that the Java representation of this API is not a public API, and could change between releases.

---

# Kubernetes API Compatibility

This API is subject to **all standard Kubernetes API versioning and compatibility requirements**.

## Versioning

**API versions follow Kubernetes conventions:**

- **`v1alpha1`**: Early development, may change without notice, not recommended for production
- **`v1beta1`**: API is well-tested but may still change in backwards-incompatible ways
- **`v1`**: Stable API with backwards compatibility guarantees

**Compatibility guarantees:**

- Within a stable API version (e.g., `v1`), changes must be backwards-compatible
- Fields can be added but not removed
- Field semantics cannot change in backwards-incompatible ways
- Deprecated fields must be supported for at least one Kubernetes minor version
- API version graduation (alpha → beta → stable) follows Kubernetes deprecation policy

## Schema Evolution

**Adding fields:**
- New optional fields can be added at any time
- New required fields require API version increment

**Removing fields:**
- Mark field as deprecated in current version
- Remove in next major API version increment
- Provide migration path in documentation

**Changing semantics:**
- Requires new API version
- Old version must be supported alongside new version during migration period

---

# Design Inspiration

## Kubernetes Gateway API Model

This API is **inspired by the Kubernetes Gateway API** (gateway.networking.k8s.io), particularly:

**Status Conditions Model:**

We use the Gateway API's pattern for communicating reconciliation state and errors through `status.conditions`:

```yaml
status:
  conditions:
    - type: Ready
      status: "True"
      observedGeneration: 2
      lastTransitionTime: "2024-01-15T10:30:00Z"
      reason: ReconciliationSucceeded
      message: "Proxy deployment is ready with 3/3 replicas"
    - type: Accepted
      status: "True"
      observedGeneration: 2
      lastTransitionTime: "2024-01-15T10:29:45Z"
      reason: Accepted
      message: "Configuration is valid"
```

**Standard condition types:**

- **`Accepted`**: Configuration has been validated and accepted
- **`Ready`**: Resource is fully reconciled and operational
- **`Programmed`**: Underlying resources (Deployment, Service) are created and configured

**Condition fields:**

- **`type`**: Condition type (e.g., "Ready", "Accepted")
- **`status`**: "True", "False", or "Unknown"
- **`observedGeneration`**: Spec generation this condition relates to (detects stale status)
- **`lastTransitionTime`**: When condition last changed
- **`reason`**: Machine-readable reason code (CamelCase)
- **`message`**: Human-readable message explaining the condition

## Multi-Resource Model

Like Gateway API, we separate concerns across multiple CRs:

- **`KafkaProxy`**: Core proxy configuration (similar to Gateway)
- **`KafkaProxyIngress`**: Network access configuration (similar to HTTPRoute)
- **`KafkaProtocolFilter`**: Filter definitions (similar to policy attachments)

This allows:
- Independent lifecycle management
- Role-based access control (different teams manage different CRs)
- Shared filter definitions across multiple proxies

---

# For CR Authors (Kubernetes Users)

This section describes how to use the CRDs to deploy and configure Kroxylicious.

## Custom Resources

**Primary CRs:**

- **`KafkaProxy`**: Defines a proxy deployment (replicas, configuration, filters, upstream cluster)
- **`KafkaProxyIngress`**: Defines how clients access the proxy (load balancer, routes)
- **`KafkaProtocolFilter`**: Defines reusable filter configurations

## Understanding Status Conditions

**Check resource readiness:**

```bash
kubectl get kafkaproxy my-proxy -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
```

**Common condition patterns:**

**Successful reconciliation:**
```yaml
conditions:
  - type: Accepted
    status: "True"
    reason: Accepted
  - type: Ready
    status: "True"
    reason: ReconciliationSucceeded
```

**Configuration error:**
```yaml
conditions:
  - type: Accepted
    status: "False"
    reason: Invalid
    message: "Filter 'my-filter' references unknown KafkaProtocolFilter 'missing-filter'"
  - type: Ready
    status: "False"
    reason: ConfigurationError
```

**Deployment not ready:**
```yaml
conditions:
  - type: Accepted
    status: "True"
    reason: Accepted
  - type: Ready
    status: "False"
    reason: DeploymentNotReady
    message: "Waiting for 3/3 replicas to be ready"
```

**Check observedGeneration:**

If `observedGeneration` is less than `metadata.generation`, the operator hasn't processed your latest changes yet.

## Example CR

```yaml
apiVersion: kroxylicious.io/v1alpha1
kind: KafkaProxy
metadata:
  name: my-proxy
spec:
  replicas: 3
  filters:
    - filterRef:
        name: record-encryption
  upstreamCluster:
    bootstrapServers: kafka.kafka.svc.cluster.local:9092
    tls:
      trustAnchorRef:
        name: kafka-ca-cert
status:
  conditions:
    - type: Accepted
      status: "True"
      observedGeneration: 1
      lastTransitionTime: "2024-01-15T10:29:45Z"
      reason: Accepted
    - type: Ready
      status: "True"
      observedGeneration: 1
      lastTransitionTime: "2024-01-15T10:30:00Z"
      reason: ReconciliationSucceeded
```

---

# For Operator Developers

This section describes requirements when working on the operator or extending the API.

## Java Classes Are Generated

**Important:** The Java classes in this module are **generated from the CRD YAML schemas**.

- **CRD schemas** (YAML) are the source of truth and the public API
- **Java classes** are generated code (do not edit directly)
- Changes to the API require updating the CRD schemas

## Maintaining Status Conditions

**When implementing reconciliation**, you must update status conditions following Gateway API patterns:

**Set Accepted condition:**

```java
var condition = new Condition();
condition.setType("Accepted");
condition.setStatus("True");
condition.setObservedGeneration(resource.getMetadata().getGeneration());
condition.setLastTransitionTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
condition.setReason("Accepted");
condition.setMessage("Configuration is valid");

resource.getStatus().setConditions(List.of(condition));
```

**Set Ready condition on success:**

```java
var readyCondition = new Condition();
readyCondition.setType("Ready");
readyCondition.setStatus("True");
readyCondition.setObservedGeneration(resource.getMetadata().getGeneration());
readyCondition.setLastTransitionTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
readyCondition.setReason("ReconciliationSucceeded");
readyCondition.setMessage("Proxy deployment is ready with 3/3 replicas");
```

**Set Ready condition on error:**

```java
var readyCondition = new Condition();
readyCondition.setType("Ready");
readyCondition.setStatus("False");
readyCondition.setObservedGeneration(resource.getMetadata().getGeneration());
readyCondition.setLastTransitionTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
readyCondition.setReason("DeploymentFailed");
readyCondition.setMessage("Failed to create deployment: " + error.getMessage());
```

## Standard Reason Codes

**Use standard reason codes** for consistency:

**Accepted condition:**
- `Accepted`: Configuration is valid
- `Invalid`: Configuration has errors
- `InvalidReference`: Referenced resource not found
- `UnsupportedValue`: Field value is not supported

**Ready condition:**
- `ReconciliationSucceeded`: Resource is ready
- `ConfigurationError`: Configuration is invalid
- `DeploymentNotReady`: Underlying deployment not ready
- `DeploymentFailed`: Failed to create/update deployment
- `ResourceError`: Error with underlying Kubernetes resources

## Condition Transition Rules

**Only update `lastTransitionTime` when status changes:**

```java
Optional<Condition> existing = findCondition(resource, "Ready");
if (existing.isEmpty() || !existing.get().getStatus().equals(newStatus)) {
    condition.setLastTransitionTime(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT));
} else {
    condition.setLastTransitionTime(existing.get().getLastTransitionTime());
}
```

**Always update `observedGeneration`:**

```java
condition.setObservedGeneration(resource.getMetadata().getGeneration());
```

## Adding New CRDs

**When adding a new CRD:**

1. Define the CRD YAML schema following Kubernetes conventions
2. Include `status.conditions` field with standard condition types
3. Generate Java classes from the schema
4. Implement controller following reconciliation patterns
5. Update documentation in `kroxylicious-docs`
6. Add integration tests
7. Follow proposal process (this is a public API change)

## API Compatibility Requirements

**When modifying CRDs:**

- ✅ Add new optional fields
- ✅ Add new condition types
- ✅ Add new reason codes
- ❌ Remove fields (deprecate first, remove in next API version)
- ❌ Change field types
- ❌ Make optional fields required
- ❌ Change field semantics

**Deprecation process:**

1. Mark field as deprecated in CRD schema and documentation
2. Continue supporting the field for at least one minor version
3. Log warning when deprecated field is used
4. Remove in next major API version
5. Provide migration guide

## Cross-References

- **Operator implementation**: See [`../kroxylicious-operator/README.md`](../kroxylicious-operator/README.md)
- **Kubernetes Gateway API**: https://gateway-api.sigs.k8s.io/
- **Kubernetes API conventions**: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md
