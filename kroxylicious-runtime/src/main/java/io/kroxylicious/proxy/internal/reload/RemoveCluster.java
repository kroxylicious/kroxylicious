/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ReconfigureError;

/**
 * Tear a virtual cluster down during a reconfigure.
 *
 * <h2>Two phases</h2>
 * <ol>
 *   <li><b>Lifecycle to {@code STOPPED}</b> — {@link VirtualClusterRegistry#removeVirtualCluster}
 *       drives the lifecycle through {@code DRAINING} to {@code STOPPED}, draining in-flight
 *       connections. The SERVING-state guard begins rejecting new connections as soon as the
 *       lifecycle leaves {@code SERVING}, so by the time we reach phase 2 no new traffic is
 *       arriving at this VC.</li>
 *   <li><b>Gateway deregistration</b> — {@link EndpointRegistry#deregisterVirtualCluster}
 *       releases the bindings. For port-addressed gateways the binding map empties and the
 *       acceptor's port is released via {@code NetworkUnbindRequest}. For SNI gateways the
 *       SNI route is removed; the acceptor stays alive for any remaining VCs.</li>
 * </ol>
 *
 */
final class RemoveCluster implements ClusterOperation {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveCluster.class);

    private static final String LOG_KEY_VIRTUAL_CLUSTER = "virtualCluster";
    private static final String LOG_KEY_ERROR = "error";

    private final VirtualClusterModel model;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;

    RemoveCluster(VirtualClusterModel model,
                  VirtualClusterRegistry virtualClusterRegistry,
                  EndpointRegistry endpointRegistry) {
        this.model = Objects.requireNonNull(model, "model");
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
    }

    @Override
    public String clusterName() {
        return model.getClusterName();
    }

    @Override
    public Operation operation() {
        return Operation.REMOVE;
    }

    @Override
    public Optional<ReconfigureError> apply() {
        try {
            virtualClusterRegistry.removeVirtualCluster(clusterName()).join();
        }
        catch (RuntimeException e) {
            return Optional.of(reportFailure(CompletionExceptions.unwrap(e),
                    "reconfigure: failed to remove virtual cluster"));
        }

        // Await every gateway's unbind
        var deregisterFutures = model.gateways().values().stream()
                .map(g -> endpointRegistry.deregisterVirtualCluster(g).toCompletableFuture())
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(deregisterFutures).join();
        }
        catch (RuntimeException e) {
            return Optional.of(reportFailure(CompletionExceptions.unwrap(e),
                    "reconfigure: failed to deregister gateways for removed virtual cluster"));
        }
        return Optional.empty();
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
