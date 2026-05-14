# kroxylicious-kubernetes-admission Module

This module implements a Kubernetes mutating admission webhook that injects Kroxylicious proxy sidecars into Kafka application pods. See also [`../README.md`](../README.md) for project-wide context.

## Purpose

The webhook automates the "sidecar injection" pattern (as used by Istio/Linkerd) for Kroxylicious. When a pod is created in an enabled namespace, the webhook mutates it to add a Kroxylicious sidecar container that proxies Kafka traffic via `localhost`.

## Trust Model

The webhook operates under a strict trust boundary between two roles:

- **Webhook administrator**: Controls what gets injected (proxy image, filters, target cluster, security context). Creates `KroxyliciousSidecarConfig` resources.
- **Application pod owner**: Can opt out of injection via labels and select a specific config by name via annotations. Cannot tamper with the proxy config, image, or security context.

The webhook always overwrites the `sidecar.kroxylicious.io/proxy-config` annotation, preventing app owners from pre-setting malicious config.

## How Injection Works

1. The `MutatingWebhookConfiguration` uses `namespaceSelector` to intercept pod CREATE requests in namespaces labelled `sidecar.kroxylicious.io/injection: enabled`.
2. The webhook resolves a `KroxyliciousSidecarConfig` for the pod's namespace.
3. If injection should proceed, the webhook generates a JSON Patch (RFC 6902) that:
   - Adds a sidecar container running the Kroxylicious proxy
   - Adds a downwardAPI volume projecting the proxy config from a pod annotation
   - Sets `KAFKA_BOOTSTRAP_SERVERS=localhost:<port>` on existing app containers
   - Sets pod-level security context if not already present
   - Adds target cluster TLS trust anchor volumes if configured

## Custom Resource: `KroxyliciousSidecarConfig`

A namespaced CRD that defines sidecar configuration per namespace. See the [CRD definition](../kroxylicious-admission-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.sidecar.kroxylicious.io-v1.yml) for the full field reference.

### Plugin Images

Third-party filter plugins are packaged as OCI images. JARs must be placed in the `/plugins` directory within the image.

How the webhook mounts plugin JARs depends on the Kubernetes cluster version:

- **OCI image volumes (Kubernetes 1.35+):** The image is mounted directly as a read-only volume. The image can be built `FROM scratch` since no executable is needed — the container runtime handles the mount.
- **EmptyDir fallback (older clusters):** The webhook creates an init container that runs the plugin image and executes `cp /plugins/*.jar /dest/` to copy JARs into an emptyDir volume. This requires the image to include at least `sh` and `cp` (e.g. a busybox base image).

If your plugin image needs to work on clusters both with and without OCI image volume support, use a base image that provides a shell:

```dockerfile
FROM quay.io/quay/busybox
COPY my-filter.jar /plugins/
```

If you know your target cluster supports OCI image volumes, a scratch-based image is sufficient:

```dockerfile
FROM scratch
COPY my-filter.jar /plugins/
```

## Labels

| Label | Purpose | Controlled by |
|-------|---------|---------------|
| `sidecar.kroxylicious.io/injection` | Namespace-level opt-in (value `enabled`) or pod-level opt-out (value `disabled`) | Admin / App owner |
| `sidecar.kroxylicious.io/injection-skipped` | Set by webhook on pods where injection was skipped, value indicates reason | Webhook |

## Annotations

| Annotation | Purpose | Controlled by |
|------------|---------|---------------|
| `sidecar.kroxylicious.io/config` | Select specific config by name | App owner |
| `sidecar.kroxylicious.io/proxy-config` | Generated proxy YAML, consumed by sidecar via downwardAPI volume | Webhook |
| `sidecar.kroxylicious.io/config-generation` | Records `metadata.generation` of the `KroxyliciousSidecarConfig` at injection time | Webhook |

## Injection Decision Logic

The injection decision follows this order:

1. **Opt-out**: Pod has label `sidecar.kroxylicious.io/injection: disabled` &rarr; skip
2. **Already injected**: Pod has a container or init container named `kroxylicious-proxy` &rarr; skip
3. **Config resolution**: The webhook resolves a `KroxyliciousSidecarConfig` for the namespace:
   - **No config** found &rarr; skip (pod labelled with reason)
   - **Multiple configs** found &rarr; skip (pod labelled with reason)
   - **Invalid config** (proxy config generation fails) &rarr; skip (pod labelled with reason)
   - **Config found and valid** &rarr; inject

## Native Sidecar Support

On Kubernetes 1.28+, the webhook injects the sidecar as an init container with `restartPolicy: Always` (native sidecar). This ensures proper startup ordering (sidecar starts before app containers) and shutdown ordering (sidecar stops after). On older clusters, the sidecar is injected as a regular container. The webhook auto-detects the cluster version at startup.

## Failure Policy

The `MutatingWebhookConfiguration` uses `failurePolicy: Fail`, meaning the API server blocks pod creation if the webhook is unreachable or returns an HTTP error. This is the standard approach for sidecar injection webhooks (consistent with Istio and Linkerd). Internal errors in the webhook cause an HTTP 500 response, which triggers this policy.

Non-error skip paths (null request, null pod, opt-out, already injected) always allow the pod.

## Uninjected Pod Policy

By default, pods that cannot be injected (no config, multiple configs, invalid config) are **admitted without a sidecar**. The `UNINJECTED_POD_POLICY` environment variable controls this behaviour:

- `Admit` (default): allow the pod without injection
- `Deny`: reject the pod with a 403 response

This is independent of the `MutatingWebhookConfiguration` `failurePolicy`, which controls the API server's behaviour when the webhook itself fails.

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
| `SidecarConfigStatusUpdater` | Updates `status.conditions` on `KroxyliciousSidecarConfig` resources |
| `KubernetesVersion` | Detects cluster version and feature gate support (native sidecars, OCI image volumes) |

## Configuration

The webhook is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BIND_ADDRESS` | `0.0.0.0:8443` | HTTPS bind address |
| `TLS_CERT_PATH` | `/etc/webhook/tls/tls.crt` | PEM certificate file |
| `TLS_KEY_PATH` | `/etc/webhook/tls/tls.key` | PEM private key file |
| `KROXYLICIOUS_IMAGE` | (required) | Default proxy container image |
| `UNINJECTED_POD_POLICY` | `Admit` | Policy for pods that cannot be injected: `Admit` (allow without sidecar) or `Deny` (reject with 403) |
| `K8S_FEATURE_GATES` | (auto-detect) | Comma-separated Kubernetes feature gate overrides, e.g. `SidecarContainers=false,ImageVolume=false`. Escape hatch for when version-based detection is insufficient; deployers are responsible for keeping it in sync with their cluster. |

## Deployment

Install manifests are in `packaging/install/`. They include namespace, RBAC, deployment, service, `MutatingWebhookConfiguration`, `PodDisruptionBudget`, and cert-manager resources. The deployment runs 2 replicas with pod anti-affinity and a PDB to ensure availability during voluntary disruptions.

**With cert-manager:**
```bash
kubectl apply -f packaging/install/
```

**Without cert-manager:** Skip `06.Certificate.kroxylicious-webhook.yaml`, create the TLS Secret manually, and patch the `MutatingWebhookConfiguration` `caBundle` field.

## Building

```bash
# Build the module
mvn clean install -pl kroxylicious-kubernetes/kroxylicious-admission -am

# Build container image (requires dist profile)
mvn clean install -pl kroxylicious-kubernetes/kroxylicious-admission -am -Pdist

# Run integration tests on Kind
mvn verify -pl kroxylicious-kubernetes/kroxylicious-admission -Pdist
```

## Testing

- **Unit tests** (`*Test.java`): Test individual classes in isolation. Run with `mvn test`.
- **KT tests** (`*KT.java`): Kubernetes integration tests requiring a real cluster and the `dist` Maven profile. Gated by `@EnabledIf` annotations that check for cluster availability.

### Running KT Tests

KT tests require:

1. **`openssl`** on `PATH` (for generating self-signed TLS certificates).
2. **Container image archives** built by the `dist` Maven profile:
   ```bash
   mvn package -pl kroxylicious-kubernetes/kroxylicious-admission -Pdist -am -DskipTests
   ```
3. **A Kubernetes cluster.** The plugin end-to-end tests (`*PluginEndToEndKT`) cover both OCI image volume mounting (Kubernetes 1.35+, `ImageVolume` feature gate) and the emptyDir fallback path. The Kind variant creates a cluster with `ImageVolume` enabled; the Minikube variant requires Kubernetes 1.35+ (the test will be skipped with an `assumeTrue` message if the version is too old).

#### Minikube

Start Minikube with containerd and ensure your current kubectl context is `minikube`:

```bash
minikube start --container-runtime=containerd
```

Then run:

```bash
mvn test -pl kroxylicious-kubernetes/kroxylicious-admission \
  -Dtest=io.kroxylicious.kubernetes.webhook.MinikubePluginEndToEndKT
```

The test loads and removes container images from the Minikube registry automatically. Requires `minikube` on `PATH`.

#### Kind

The Kind variant creates and deletes a dedicated cluster with the `ImageVolume` feature gate enabled:

```bash
mvn test -pl kroxylicious-kubernetes/kroxylicious-admission \
  -Dtest=io.kroxylicious.kubernetes.webhook.KindPluginEndToEndKT
```

Requires `kind` on `PATH`.

#### Webhook Install Tests

The webhook install tests (`*WebhookInstallKT`) verify manifest installation and sidecar injection without deploying Kafka. They have the same cluster requirements but do not need the `ImageVolume` feature gate. Run with:

```bash
mvn test -pl kroxylicious-kubernetes/kroxylicious-admission \
  -Dtest=io.kroxylicious.kubernetes.webhook.MinikubeWebhookInstallKT
```

## Cross-References

- **CRD definition**: See `../kroxylicious-admission-api/src/main/resources/META-INF/fabric8/kroxylicioussidecarconfigs.sidecar.kroxylicious.io-v1.yml`
- **Proxy configuration model**: See `../kroxylicious-runtime/`
- **Operator (standalone proxy deployment)**: See [`../kroxylicious-operator/README.md`](../kroxylicious-operator/README.md)
- **Security patterns**: See [`../.claude/rules/security-patterns.md`](../.claude/rules/security-patterns.md)
