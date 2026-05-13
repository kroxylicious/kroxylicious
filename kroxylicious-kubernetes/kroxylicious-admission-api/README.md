# kroxylicious-admission-api Module

This module defines the Kubernetes Custom Resource Definition (CRD) for the Kroxylicious sidecar injection webhook. See also [`../README.md`](../README.md) for project-wide context.

## API Roles

**CR Authors**: Kubernetes users who create and manage `KroxyliciousSidecarConfig` custom resources to configure sidecar injection in their namespaces.

**Webhook Developers**: Kroxylicious developers working on the admission webhook (in the `kroxylicious-admission` module) that consumes these CRs. 3rd party developers should note that the Java representation of this API is not a public API, and could change between releases.

---

# Custom Resource: `KroxyliciousSidecarConfig`

**API Group**: `sidecar.kroxylicious.io`
**Version**: `v1alpha1`
**Scope**: Namespaced
**Short name**: `ksc`

A `KroxyliciousSidecarConfig` defines how the sidecar injection webhook should configure the Kroxylicious proxy container it injects into pods in the resource's namespace.

## Example CR

```yaml
apiVersion: sidecar.kroxylicious.io/v1alpha1
kind: KroxyliciousSidecarConfig
metadata:
  name: my-sidecar-config
spec:
  virtualClusters:
    - name: my-cluster
      targetBootstrapServers: kafka.kafka.svc.cluster.local:9092
      targetClusterTls:
        trustAnchorSecretRef:
          name: kafka-ca-cert
          key: ca.crt
  filterDefinitions:
    - name: record-encryption
      type: io.kroxylicious.filter.encryption.RecordEncryptionFilterFactory
      config:
        kmsType: vault
status:
  observedGeneration: 1
  conditions:
    - type: Ready
      status: "True"
      observedGeneration: 1
      lastTransitionTime: "2025-03-15T10:30:00Z"
      reason: Accepted
      message: ""
```

## Spec Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `virtualClusters` | array (max 1) | Yes | Target Kafka cluster configuration |
| `filterDefinitions` | array | No | Filter plugins applied to proxied traffic |
| `plugins` | array | No | Third-party plugin OCI images |
| `secretMounts` | array | No | Secrets to mount in the sidecar container |
| `proxyImage` | string | No | Override for the sidecar container image |
| `managementPort` | integer | No | Management endpoint port (default: 9082) |
| `resources` | object | No | Resource requests/limits for the sidecar container |
| `setBootstrapEnvVar` | boolean | No | Whether to set `KAFKA_BOOTSTRAP_SERVERS` on app containers (default: true) |

See the [CRD YAML](src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.sidecar.kroxylicious.io-v1.yml) for full field documentation and validation rules.

## Status Conditions

The webhook updates `status.conditions` to communicate validation state:

**Accepted (Ready=True):**
```yaml
conditions:
  - type: Ready
    status: "True"
    reason: Accepted
    message: ""
```

**Invalid (Ready=False):**
```yaml
conditions:
  - type: Ready
    status: "False"
    reason: Invalid
    message: "proxy config generation failed: ..."
```

If `observedGeneration` is less than `metadata.generation`, the webhook hasn't processed the latest changes yet.

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
- API version graduation (alpha -> beta -> stable) follows Kubernetes deprecation policy

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

This API is **inspired by the Kubernetes Gateway API** (gateway.networking.k8s.io), particularly its status conditions model for communicating reconciliation state and errors.

**Condition fields:**

- **`type`**: Condition type (currently only `Ready`)
- **`status`**: `"True"`, `"False"`, or `"Unknown"`
- **`observedGeneration`**: Spec generation this condition relates to (detects stale status)
- **`lastTransitionTime`**: When condition last changed
- **`reason`**: Machine-readable reason code (CamelCase)
- **`message`**: Human-readable message explaining the condition

---

# For Webhook Developers

## Java Classes Are Generated

**Important:** The Java classes in this module are **generated from the CRD YAML schema**.

- **CRD schema** (YAML) is the source of truth and the public API
- **Java classes** are generated code (do not edit directly)
- Changes to the API require updating the CRD schema
- The one exception is `Condition.java`, which is hand-written

## Maintaining Status Conditions

When updating status, follow Gateway API patterns:

**Set Ready condition on acceptance:**

```java
Condition readyCondition = new ConditionBuilder()
        .withType(Condition.Type.Ready)
        .withStatus(Condition.Status.TRUE)
        .withReason("Accepted")
        .withMessage("")
        .withLastTransitionTime(clock.instant())
        .withObservedGeneration(generation)
        .build();
```

**Set Ready condition on validation failure:**

```java
Condition readyCondition = new ConditionBuilder()
        .withType(Condition.Type.Ready)
        .withStatus(Condition.Status.FALSE)
        .withReason("Invalid")
        .withMessage(validationError)
        .withLastTransitionTime(clock.instant())
        .withObservedGeneration(generation)
        .build();
```

## Standard Reason Codes

**Ready condition:**
- `Accepted`: Configuration is valid
- `Invalid`: Configuration has validation errors

## API Compatibility Requirements

**When modifying the CRD:**

- Add new optional fields
- Add new condition types
- Add new reason codes
- Do not remove fields (deprecate first, remove in next API version)
- Do not change field types
- Do not make optional fields required
- Do not change field semantics

## Cross-References

- **Webhook implementation**: See [`../kroxylicious-admission/README.md`](../kroxylicious-admission/README.md)
- **Operator API (KafkaProxy, KafkaProxyIngress, etc.)**: See [`../kroxylicious-kubernetes-api/README.md`](../kroxylicious-kubernetes-api/README.md)
- **Kubernetes Gateway API**: https://gateway-api.sigs.k8s.io/
- **Kubernetes API conventions**: https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md
