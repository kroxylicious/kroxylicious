/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ReconfigureError;

/**
 * Bring a new virtual cluster online during a reconfigure.
 *
 * <h2>Three phases</h2>
 * <ol>
 *   <li><b>Lifecycle bookkeeping</b> — {@link VirtualClusterRegistry#addVirtualCluster}
 *       creates the lifecycle in {@code INITIALIZING}. This MUST happen first: the
 *       SERVING-state guard in {@code KafkaProxyInitializer} rejects new connections to
 *       the binding until step 3 transitions the lifecycle to {@code SERVING}. If we bound
 *       before bookkeeping, traffic could land on a VC the proxy didn't yet know existed.</li>
 *   <li><b>Gateway registration</b> — {@link EndpointRegistry#registerVirtualCluster} binds
 *       each gateway. For port-addressed gateways this opens a fresh acceptor channel; for
 *       SNI gateways it attaches to an existing acceptor via the {@code computeIfAbsent}
 *       short-circuit.</li>
 *   <li><b>{@code INITIALIZING → SERVING}</b> — only after every gateway is bound.</li>
 * </ol>
 *
 * <h2>Failure modes</h2>
 * <ul>
 *   <li>Phase 1 fails ⇒ no lifecycle was created; no rollback needed. Report error.</li>
 *   <li>Phase 2 fails ⇒ drive lifecycle to {@code STOPPED} via {@code FAILED}, then
 *       deregister every gateway to roll back any partially-applied bindings. The
 *       {@link ReconfigureError} carries the original bind cause, not a rollback cause.</li>
 *   <li>Phase 3 is a synchronous registry state change — failure here means the registry
 *       is in an inconsistent state and the orchestrator will surface it as an error.</li>
 * </ul>
 */
final class AddCluster implements ClusterOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(AddCluster.class);

    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";
    private static final String LOG_KEY_ERROR = "error";

    private final String clusterName;
    private final Supplier<VirtualClusterModel> modelSupplier;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;

    AddCluster(String clusterName,
               Supplier<VirtualClusterModel> modelSupplier,
               VirtualClusterRegistry virtualClusterRegistry,
               EndpointRegistry endpointRegistry) {
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.modelSupplier = Objects.requireNonNull(modelSupplier, "modelSupplier");
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public Optional<ReconfigureError> apply() {
        // Construct the model lazily: the supplier closes over (Configuration, clusterName) and
        // calling get() triggers VCM construction including each filter's initialize(). Failures
        // here (e.g. PluginConfigurationException from a bad filter config) become a per-cluster
        // ReconfigureError rather than failing the whole reconfigure exceptionally — other
        // operations in the same plan can still succeed.
        VirtualClusterModel model;
        try {
            model = modelSupplier.get();
        }
        catch (RuntimeException e) {
            return Optional.of(reportFailure(CompletionExceptions.unwrap(e),
                    "reconfigure: failed to construct virtual cluster model"));
        }

        try {
            virtualClusterRegistry.addVirtualCluster(model).join();
        }
        catch (RuntimeException e) {
            return Optional.of(reportFailure(CompletionExceptions.unwrap(e),
                    "reconfigure: failed to create lifecycle for virtual cluster"));
        }
        return bindGatewaysWithRollback(model);
    }

    private Optional<ReconfigureError> bindGatewaysWithRollback(VirtualClusterModel model) {
        List<EndpointGateway> gateways = List.copyOf(model.gateways().values());
        var bindFutures = gateways.stream()
                .map(g -> endpointRegistry.registerVirtualCluster(g).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(bindFutures).join();
            virtualClusterRegistry.initializationSucceeded(clusterName());
            return Optional.empty();
        }
        catch (RuntimeException bindError) {
            Throwable cause = CompletionExceptions.unwrap(bindError);
            LOGGER.atWarn()
                    .setCause(cause)
                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, clusterName())
                    .addKeyValue(LOG_KEY_ERROR, cause.getMessage())
                    .log("reconfigure: gateway registration failed; rolling back");
            virtualClusterRegistry.initializationFailed(clusterName(), cause);
            rollbackVirtualClusterRegistration(gateways);
            return Optional.of(new ReconfigureError(clusterName(), cause));
        }
    }

    /**
     * Awaits every gateway's deregister before returning, so the caller sees a fully-attempted
     * rollback. Per-gateway failures are logged via {@code .exceptionally()} and swallowed —
     * the original bind cause is what we surface as the {@link ReconfigureError}, never a
     * rollback cause.
     */
    private void rollbackVirtualClusterRegistration(List<EndpointGateway> gateways) {
        var rollbackFutures = gateways.stream()
                .map(g -> endpointRegistry.deregisterVirtualCluster(g).toCompletableFuture()
                        .exceptionally(ex -> {
                            LOGGER.atWarn()
                                    .setCause(LOGGER.isDebugEnabled() ? ex : null)
                                    .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, clusterName())
                                    .addKeyValue(LOG_KEY_ERROR, ex.getMessage())
                                    .log(LOGGER.isDebugEnabled()
                                            ? "reconfigure: rollback deregister failed; gateway binding may remain active"
                                            : "reconfigure: rollback deregister failed; gateway binding may remain active. Raise log level to DEBUG to see the stack.");
                            return null;
                        }))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(rollbackFutures).join();
    }

    private ReconfigureError reportFailure(Throwable cause, String message) {
        LOGGER.atWarn()
                .setCause(cause)
                .addKeyValue(LOG_KEY_VIRTUAL_CLUSTER, clusterName())
                .addKeyValue(LOG_KEY_ERROR, cause.getMessage())
                .log(message);
        return new ReconfigureError(clusterName(), cause);
    }
}
