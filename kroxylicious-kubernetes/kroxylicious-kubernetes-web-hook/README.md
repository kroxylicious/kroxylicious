# kroxylicious-kubernetes-web-hook Module

This module implements a Kubernetes mutating admission webhook that injects Kroxylicious proxy sidecars into Kafka application pods. See also [`../README.md`](../README.md) for project-wide context.

## Purpose

The webhook automates the "sidecar injection" pattern (as used by Istio/Linkerd) for Kroxylicious. When a pod is created in an enabled namespace, the webhook mutates it to add a Kroxylicious sidecar container that proxies Kafka traffic via `localhost`.

## Trust Model

The webhook operates under a strict trust boundary between two roles:

- **Webhook administrator**: Controls what gets injected (proxy image, filters, upstream cluster, security context). Creates `KroxyliciousSidecarConfig` resources.
- **Application pod owner**: Can only influence injection through explicitly delegated annotations. Cannot tamper with the proxy config, image, or security context.

The webhook always overwrites the `kroxylicious.io/proxy-config` annotation, preventing app owners from pre-setting malicious config.

## How Injection Works

1. The `MutatingWebhookConfiguration` uses `namespaceSelector` to intercept pod CREATE requests in namespaces labelled `kroxylicious.io/sidecar-injection: enabled`.
2. The webhook resolves a `KroxyliciousSidecarConfig` for the pod's namespace.
3. If injection should proceed, the webhook generates a JSON Patch (RFC 6902) that:
   - Adds a sidecar container running the Kroxylicious proxy
   - Adds a downwardAPI volume projecting the proxy config from a pod annotation
   - Sets `KAFKA_BOOTSTRAP_SERVERS=localhost:<port>` on existing app containers
   - Sets pod-level security context if not already present
   - Adds upstream TLS trust anchor volumes if configured

## Custom Resource: `KroxyliciousSidecarConfig`

A namespaced CRD (group `kroxylicious.io`, version `v1alpha1`) that defines sidecar configuration per namespace. Key fields:

| Field | Description |
|-------|-------------|
| `upstreamBootstrapServers` | Bootstrap servers of the upstream Kafka cluster (required) |
| `proxyImage` | Override the proxy container image |
| `bootstrapPort` | Proxy bootstrap port on localhost (default: 19092) |
| `nodeIdRange` | Broker node ID range (default: 0-2) |
| `managementPort` | Management endpoint port (default: 9190) |
| `filterDefinitions` | Filters applied to proxied traffic |
| `upstreamTls` | TLS configuration for the upstream Kafka connection |
| `delegatedAnnotations` | Annotations app owners may set to override config |
| `setBootstrapEnvVar` | Whether to set `KAFKA_BOOTSTRAP_SERVERS` on app containers (default: true) |
| `resources` | Resource requests/limits for the sidecar container |

## Annotations

| Annotation | Purpose | Controlled by |
|------------|---------|---------------|
| `kroxylicious.io/sidecar-injection` | Namespace-level opt-in (label) | Admin |
| `kroxylicious.io/inject-sidecar` | Pod-level opt-out (`"false"`) | App owner |
| `kroxylicious.io/sidecar-config` | Select specific config by name | App owner |
| `kroxylicious.io/proxy-config` | Generated proxy YAML (set by webhook) | Webhook |
| `kroxylicious.io/sidecar-status` | Injection status (set by webhook) | Webhook |
| `kroxylicious.io/sidecar-bootstrap-port` | Override bootstrap port (if delegated) | App owner |
| `kroxylicious.io/sidecar-node-id-range` | Override node ID range (if delegated) | App owner |

## Injection Decision Logic

The injection decision follows this order:

1. **Opt-out**: Pod has annotation `kroxylicious.io/inject-sidecar: "false"` &rarr; skip
2. **Already injected**: Pod has a container named `kroxylicious-proxy` &rarr; skip
3. **No config**: No `KroxyliciousSidecarConfig` found for the namespace &rarr; skip
4. Otherwise &rarr; inject

## Delegated Annotations

The admin can list annotation keys in `delegatedAnnotations` to allow app owners to override specific settings. If an app owner sets a `kroxylicious.io/` annotation that is not delegated, it is ignored and a warning is logged.

## Native Sidecar Support

On Kubernetes 1.28+, the webhook injects the sidecar as an init container with `restartPolicy: Always` (native sidecar). This ensures proper startup ordering (sidecar starts before app containers) and shutdown ordering (sidecar stops after). On older clusters, the sidecar is injected as a regular container. The webhook auto-detects the cluster version at startup.

## Fail-Open Semantics

The webhook always returns `allowed: true`, even on internal errors. If the webhook is unavailable, the `MutatingWebhookConfiguration` uses `failurePolicy: Ignore` so pods are created without sidecars. This is the standard approach for sidecar injection webhooks.

## Key Classes

| Class | Purpose |
|-------|---------|
| `WebhookMain` | Entrypoint; creates HTTPS server, fabric8 client, config resolver |
| `WebhookServer` | `HttpsServer` lifecycle, TLS from PEM cert/key files |
| `AdmissionHandler` | `POST /mutate` handler; deserialises admission review, delegates to mutator |
| `InjectionDecision` | Pure function determining whether to inject |
| `PodMutator` | Generates JSON Patch operations for pod mutation |
| `ProxyConfigGenerator` | Generates proxy configuration YAML from `KroxyliciousSidecarConfigSpec` |
| `SidecarConfigResolver` | Fabric8 informer-based cache of `KroxyliciousSidecarConfig` resources |

## Configuration

The webhook is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BIND_ADDRESS` | `0.0.0.0:8443` | HTTPS bind address |
| `TLS_CERT_PATH` | `/etc/webhook/tls/tls.crt` | PEM certificate file |
| `TLS_KEY_PATH` | `/etc/webhook/tls/tls.key` | PEM private key file |
| `KROXYLICIOUS_IMAGE` | (required) | Default proxy container image |

## Deployment

Install manifests are in `packaging/install/`. They include namespace, RBAC, deployment, service, `MutatingWebhookConfiguration`, and cert-manager resources.

**With cert-manager:**
```bash
kubectl apply -f packaging/install/
```

**Without cert-manager:** Skip `06.Certificate.kroxylicious-webhook.yaml`, create the TLS Secret manually, and patch the `MutatingWebhookConfiguration` `caBundle` field.

## Building

```bash
# Build the module
mvn clean install -pl kroxylicious-kubernetes-web-hook -am

# Build container image (requires dist profile)
mvn clean install -pl kroxylicious-kubernetes-web-hook -am -Pdist

# Run integration tests on Kind
mvn verify -pl kroxylicious-kubernetes-web-hook -Pdist
```

## Testing

- **Unit tests** (`*Test.java`): Test individual classes in isolation. Run with `mvn test`.
- **KT tests** (`*KT.java`): Kubernetes integration tests requiring a real cluster and the `dist` Maven profile. Gated by `@EnabledIf` annotations that check for cluster availability.

### Running KT Tests

KT tests require:

1. **`openssl`** on `PATH` (for generating self-signed TLS certificates).
2. **Container image archives** built by the `dist` Maven profile:
   ```bash
   mvn package -pl kroxylicious-kubernetes/kroxylicious-kubernetes-web-hook -Pdist -am -DskipTests
   ```
3. **A Kubernetes cluster whose container runtime supports OCI image volumes.** The plugin end-to-end test (`*PluginEndToEndKT`) mounts third-party plugin JARs as image volumes (Kubernetes `ImageVolume` feature gate, beta since 1.33). This requires **containerd 2.0+** or **CRI-O**; the Docker runtime does not support image volumes.

#### Minikube

Start Minikube with containerd and ensure your current kubectl context is `minikube`:

```bash
minikube start --container-runtime=containerd
```

Then run:

```bash
mvn test -pl kroxylicious-kubernetes/kroxylicious-kubernetes-web-hook \
  -Dtest=io.kroxylicious.kubernetes.webhook.MinikubePluginEndToEndKT
```

The test loads and removes container images from the Minikube registry automatically. Requires `minikube` on `PATH`.

#### Kind

The Kind variant creates and deletes a dedicated cluster with the `ImageVolume` feature gate enabled:

```bash
mvn test -pl kroxylicious-kubernetes/kroxylicious-kubernetes-web-hook \
  -Dtest=io.kroxylicious.kubernetes.webhook.KindPluginEndToEndKT
```

Requires `kind` on `PATH`.

#### Webhook Install Tests

The webhook install tests (`*WebhookInstallKT`) verify manifest installation and sidecar injection without deploying Kafka. They have the same cluster requirements but do not need the `ImageVolume` feature gate. Run with:

```bash
mvn test -pl kroxylicious-kubernetes/kroxylicious-kubernetes-web-hook \
  -Dtest=io.kroxylicious.kubernetes.webhook.MinikubeWebhookInstallKT
```

## Cross-References

- **CRD definition**: See `../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.kroxylicious.io-v1.yml`
- **Proxy configuration model**: See `../kroxylicious-runtime/`
- **Operator (standalone proxy deployment)**: See [`../kroxylicious-operator/README.md`](../kroxylicious-operator/README.md)
- **Security patterns**: See [`../.claude/rules/security-patterns.md`](../.claude/rules/security-patterns.md)
