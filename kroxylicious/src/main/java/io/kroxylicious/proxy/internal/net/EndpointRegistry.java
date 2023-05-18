/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;

/**
 * The endpoint registry is responsible for associating network endpoints with broker/bootstrap addresses of virtual clusters.
 * Pictorially the registry looks like this:
 *
 * <pre><code>
 * Registry
 *    ├─ Endpoint (tls 9092)
 *    │    ╰───┬──→ cluster1.kafka.com         ──→ Virtual Cluster A bootstrap
 *    │        ├──→ broker1-cluster1.kafka.com ──→ Virtual Cluster A broker node 1
 *    │        ╰──→ broker2-cluster1.kafka.com ──→ Virtual Cluster A broker node 2
 *    ├─ Endpoint (plain 19092)
 *    │        ╰───→  (null)                   ──→ Virtual Cluster B bootstrap
 *    ├─ Endpoint (plain 19093)
 *    │        ╰───→  (null)                   ──→ Virtual Cluster B broker 1
 *    ╰─ Endpoint (plain 12094)
 *             ╰───→  (null)                   ──→ Virtual Cluster B broker 2
 * </code></pre>
 *
 *  Key points about the implementation:
 *  <ul>
 *    <li>The registry does not take direct responsibility for binding and unbinding network sockets.  Instead, it exposes
 * a queue of network binding operation events {@link NetworkBindingOperation}.  Callers get the next network
 * event by calling the {@link #takeNetworkBindingEvent()}.  Once the underlying network operation is complete, the caller
 * must complete the provided future.</li>
 *    <li>The registry exposes methods for the registration {@link #registerVirtualCluster(VirtualCluster)} and deregistration
 * {@link #deregisterVirtualCluster(VirtualCluster)} of virtual clusters.  The registry emits the required network binding
 * operations to expose the virtual cluster to the network.  These API calls return futures that will complete once
 * the underlying network operations are completed.</li>
 *    <li>The registry provides an {@link VirtualClusterBindingResolver}.  The {@link VirtualClusterBindingResolver#resolve(Endpoint, String)} method accepts
 * connection metadata (port, SNI etc) and resolves this to a @{@link VirtualClusterBinding}.  This allows
 * Kroxylicious to determine the destination of any incoming connection.</li>
 * </ul>
 * The registry is thread safe for all operations.
 * <ul>
 *    <li>virtual cluster registration uses java.util.concurrent features to ensure registration is single threaded.</li>
 *    <li>virtual cluster de-registration uses java.util.concurrent.atomic to ensure de-registration is single threaded.</li>
 *    <li>updates to the binding mapping (attached to channel) are made only whilst holding an intrinsic lock on the {@link ListeningChannelRecord}.</li>
 *    <li>updates to the binding mapping are published safely to readers (i.e. threads calling {@link VirtualClusterBindingResolver#resolve(Endpoint, String)}).  This relies the
 *    fact that the binding map uses concurrency safe data structures ({@link ConcurrentHashMap} and the exclusive use of immutable objects within it.</li>
 * </ul>
 */
public class EndpointRegistry implements AutoCloseable, VirtualClusterBindingResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointRegistry.class);
    private final AtomicBoolean registryClosed = new AtomicBoolean(false);

    interface RoutingKey {
        RoutingKey NULL_ROUTING_KEY = new NullRoutingKey();

        static RoutingKey createBindingKey(String sniHostname) {
            if (sniHostname == null || sniHostname.isEmpty()) {
                return NULL_ROUTING_KEY;
            }
            return new SniRoutingKey(sniHostname);
        }

    }

    private static class NullRoutingKey implements RoutingKey {
        @Override
        public String toString() {
            return "NullRoutingKey[]";
        }
    }

    private record SniRoutingKey(String sniHostname) implements RoutingKey {

    private SniRoutingKey(String sniHostname) {
            Objects.requireNonNull(sniHostname);
            this.sniHostname = sniHostname.toLowerCase(Locale.ROOT);
        }}

    protected static final AttributeKey<Map<RoutingKey, VirtualClusterBinding>> CHANNEL_BINDINGS = AttributeKey.newInstance("channelBindings");

    private record VirtualClusterRecord(CompletionStage<Endpoint> registrationStage, AtomicReference<CompletionStage<Void>> deregistrationStage) {

    private static VirtualClusterRecord create(CompletionStage<Endpoint> stage) {
            return new VirtualClusterRecord(stage, new AtomicReference<>());
        }}

    /** Queue of network binding operations */
    private final BlockingQueue<NetworkBindingOperation<?>> queue = new LinkedBlockingQueue<>();

    /** Registry of virtual clusters that have been registered */
    private final Map<VirtualCluster, VirtualClusterRecord> registeredVirtualClusters = new ConcurrentHashMap<>();

    private record ListeningChannelRecord(CompletionStage<Channel> bindingStage, AtomicReference<CompletionStage<Void>> unbindingStage) {

    public static ListeningChannelRecord create(CompletionStage<Channel> stage) {
            return new ListeningChannelRecord(stage, new AtomicReference<>());
        }}

    /** Registry of endpoints and their underlying Netty Channel */
    private final Map<Endpoint, ListeningChannelRecord> listeningChannels = new ConcurrentHashMap<>();

    /**
     * Blocking operation that gets the next binding operation.  Once the network operation is complete,
     * the caller must complete the {@link NetworkBindingOperation#getFuture()}.
     *
     * @return network binding operation
     *
     * @throws InterruptedException - operation interrupted
     */
    public NetworkBindingOperation<?> takeNetworkBindingEvent() throws InterruptedException {
        return queue.take();
    }

    /* test */ int countNetworkEvents() {
        return queue.size();
    }

    /**
     * Registers a virtual cluster with the registry.  The registry refers to the endpoint configuration
     * of the virtual cluster to understand its port requirements, binding new listening ports as necessary.
     * This operation returns a {@link CompletionStage<Endpoint>}.  The completion stage will complete once
     * all necessary network bind operations are completed.  The returned {@link Endpoint} refers to the
     * bootstrap endpoint of the virtual cluster.
     *
     * @param virtualCluster virtual cluster to be registered.
     * @return completion stage that will complete after registration is finished.
     */
    public CompletionStage<Endpoint> registerVirtualCluster(VirtualCluster virtualCluster) {
        Objects.requireNonNull(virtualCluster, "virtualCluster cannot be null");

        var vcr = VirtualClusterRecord.create(new CompletableFuture<Endpoint>());
        var current = registeredVirtualClusters.putIfAbsent(virtualCluster, vcr);
        if (current != null) {
            var deregistration = current.deregistrationStage().get();
            if (deregistration != null) {
                // the cluster is already being deregistered, so perform this work after it completes.
                return deregistration.thenCompose(u -> registerVirtualCluster(virtualCluster));
            }
            return current.registrationStage();
        }

        var bootstrapEndpointFuture = createEndpointAndBind(virtualCluster.getClusterBootstrapAddress(), null, virtualCluster);

        var initialBindings = new LinkedHashMap<HostPort, Integer>();
        for (int i = 0; i < virtualCluster.getNumberOfBrokerEndpointsToPrebind(); i++) {
            if (!virtualCluster.getBrokerAddress(i).equals(virtualCluster.getClusterBootstrapAddress())) {
                initialBindings.put(virtualCluster.getBrokerAddress(i), i);
            }
        }

        var brokerEndpointFutures = initialBindings.entrySet().stream().map(e -> createEndpointAndBind(e.getKey(), e.getValue(), virtualCluster)).toList();
        var allBrokerEndpointFutures = allOfFutures(brokerEndpointFutures.stream());

        var unused = CompletableFuture.allOf(bootstrapEndpointFuture, allBrokerEndpointFutures).whenComplete((u, t) -> {
            var future = vcr.registrationStage.toCompletableFuture();
            if (t != null) {
                // Try to roll back any bindings that were successfully made
                deregisterBinding(virtualCluster, (vcb) -> vcb.virtualCluster().equals(virtualCluster))
                        .handle((u1, t1) -> {
                            if (t1 != null) {
                                LOGGER.warn("Registration error", t);
                                LOGGER.warn("Secondary error occurred whilst handling a previous registration error: {}", t.getMessage(), t1);
                            }
                            registeredVirtualClusters.remove(virtualCluster);
                            future.completeExceptionally(t);
                            return null;
                        });
            }
            else {
                try {
                    future.complete(bootstrapEndpointFuture.get());
                }
                catch (Throwable t1) {
                    future.completeExceptionally(t1);
                }
            }
        });

        return vcr.registrationStage();
    }

    private CompletableFuture<Endpoint> createEndpointAndBind(HostPort hostPort, Integer nodeId, VirtualCluster virtualCluster) {
        var key = Endpoint.createEndpoint(virtualCluster.getBindAddress().orElse(null), hostPort.port(), virtualCluster.isUseTls());
        VirtualClusterBinding virtualClusterBinding = VirtualClusterBinding.createBinding(virtualCluster, nodeId);
        return registerBinding(key, hostPort.host(), virtualClusterBinding).toCompletableFuture();
        // TODO cache endpoints when future completes
    }

    /**
     * De-registers a virtual cluster from the registry, removing all existing endpoint bindings, and
     * closing any listening sockets that are no longer required. This operation returns a {@link CompletionStage<Endpoint>}.
     * The completion stage will complete once any necessary network unbind operations are completed.
     *
     * @param virtualCluster virtual cluster to be deregistered.
     * @return completion stage that will complete after registration is finished.
     */
    public CompletionStage<Void> deregisterVirtualCluster(VirtualCluster virtualCluster) {
        Objects.requireNonNull(virtualCluster, "virtualCluster cannot be null");

        var vcr = registeredVirtualClusters.get(virtualCluster);
        if (vcr == null) {
            // cluster not currently registered
            return CompletableFuture.completedFuture(null);
        }

        var deregisterFuture = new CompletableFuture<Void>();
        var updated = vcr.deregistrationStage().compareAndSet(null, deregisterFuture);
        if (!updated) {
            // cluster de-registration already in progress.
            return vcr.deregistrationStage().get();
        }

        var unused = vcr.registrationStage()
                .thenCompose((u) -> deregisterBinding(virtualCluster, binding -> binding.virtualCluster().equals(virtualCluster))
                        .handle((unused1, t) -> {
                            registeredVirtualClusters.remove(virtualCluster);
                            if (t != null) {
                                deregisterFuture.completeExceptionally(t);
                            }
                            else {
                                deregisterFuture.complete(null);
                            }
                            return null;
                        }));
        return deregisterFuture;
    }

    /* test */ boolean isRegistered(VirtualCluster virtualCluster) {
        return registeredVirtualClusters.containsKey(virtualCluster);
    }

    /* test */ int listeningChannelCount() {
        return listeningChannels.size();
    }

    private CompletionStage<Endpoint> registerBinding(Endpoint key, String host, VirtualClusterBinding virtualClusterBinding) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(virtualClusterBinding, "virtualClusterBinding cannot be null");
        var virtualCluster = virtualClusterBinding.virtualCluster();

        var lcr = listeningChannels.computeIfAbsent(key, k -> {
            // the listening channel doesn't exist, atomically create a record to represent it and request its binding to the network.
            var future = new CompletableFuture<Channel>();
            var r = ListeningChannelRecord.create(future.exceptionally(t -> {
                // Handles the case where the network bind fails
                listeningChannels.remove(key);
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                else {
                    throw new RuntimeException(t);
                }
            }));

            queue.add(new NetworkBindRequest(future, new Endpoint(key.bindingAddress(), key.port(), virtualCluster.isUseTls())));
            return r;
        });

        if (lcr.unbindingStage().get() != null) {
            // Listening channel already being unbound, chain this request to the unbind stage so it completes later.
            return lcr.unbindingStage().get().thenCompose(u -> registerBinding(key, host, virtualClusterBinding));
        }

        return lcr.bindingStage().thenApply(acceptorChannel -> {
            synchronized (lcr) {
                var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
                bindings.setIfAbsent(new ConcurrentHashMap<>());
                var bindingMap = bindings.get();
                var bindingKey = virtualCluster.requiresTls() ? RoutingKey.createBindingKey(host) : RoutingKey.NULL_ROUTING_KEY;

                // we use a bindingMap attached to the channel to record the bindings to the channel. the #deregisterBinding path
                // knows to tear down the acceptorChannel when the map becomes empty.
                var existing = bindingMap.putIfAbsent(bindingKey, virtualClusterBinding);

                if (existing != null) {
                    throw new EndpointBindingException(
                            "Endpoint %s cannot be bound with key %s, that key is already bound to %s".formatted(key, bindingKey, virtualClusterBinding));
                }

                return key;
            }
        });
    }

    private CompletionStage<Void> deregisterBinding(VirtualCluster virtualCluster, Predicate<VirtualClusterBinding> predicate) {
        Objects.requireNonNull(virtualCluster, "virtualCluster cannot be null");
        Objects.requireNonNull(predicate, "predicate cannot be null");

        // Search the listening channels for bindings matching the predicate. We could cache more information on the vcr to optimise.
        var unbindStages = listeningChannels.entrySet().stream()
                .map((e) -> {
                    var endpoint = e.getKey();
                    var lcr = e.getValue();
                    return lcr.bindingStage().thenCompose(acceptorChannel -> {
                        synchronized (lcr) {
                            var bindingMap = acceptorChannel.attr(EndpointRegistry.CHANNEL_BINDINGS).get();
                            var allEntries = bindingMap.entrySet();
                            var toRemove = allEntries.stream().filter(be -> predicate.test(be.getValue())).collect(Collectors.toSet());
                            // If our removal leaves the channel without bindings, trigger its unbinding
                            if (allEntries.removeAll(toRemove) && bindingMap.isEmpty()) {
                                var unbindFuture = new CompletableFuture<Void>();
                                var afterUnbind = unbindFuture.whenComplete((u, t) -> listeningChannels.remove(endpoint));
                                if (lcr.unbindingStage().compareAndSet(null, afterUnbind)) {
                                    queue.add(new NetworkUnbindRequest(virtualCluster.isUseTls(), acceptorChannel, unbindFuture));
                                    return afterUnbind;
                                }
                                else {
                                    return lcr.unbindingStage().get();
                                }
                            }
                            else {
                                return CompletableFuture.completedStage(null);
                            }
                        }
                    });
                });

        return allOfStage(unbindStages);
    }

    /**
     * Uses channel metadata (port, SNI name etc.) from the incoming connection to resolve a {@link VirtualClusterBinding}.
     *
     * @param endpoint    endpoint being resolved
     * @param sniHostname SNI hostname, may be null.
     * @return completion stage yielding the {@link VirtualClusterBinding} or exceptionally a EndpointResolutionException.
     */
    @Override
    public CompletionStage<VirtualClusterBinding> resolve(Endpoint endpoint, String sniHostname) {
        var lcr = this.listeningChannels.get(endpoint);
        if (lcr == null || lcr.unbindingStage().get() != null) {
            return CompletableFuture.failedStage(buildEndpointResolutionException("Failed to find channel matching ", endpoint, sniHostname));
        }
        return lcr.bindingStage().thenApply(acceptorChannel -> {
            var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
            if (bindings == null || bindings.get() == null) {
                throw buildEndpointResolutionException("No channel bindings found for ", endpoint, sniHostname);
            }
            // We first look for a binding matching by SNI name, then fallback to a null match.
            var binding = bindings.get().getOrDefault(RoutingKey.createBindingKey(sniHostname), bindings.get().get(RoutingKey.NULL_ROUTING_KEY));
            if (binding == null) {
                throw buildEndpointResolutionException("No channel bindings found for ", endpoint, sniHostname);
            }
            return binding;
        });
    }

    private EndpointResolutionException buildEndpointResolutionException(String prefix, Endpoint endpoint, String sniHostname) {
        return new EndpointResolutionException(
                ("%s binding address: %s, port %d, sniHostname: %s, tls %s").formatted(prefix,
                        endpoint.bindingAddress() == null ? "any" : endpoint.bindingAddress(),
                        endpoint.port(),
                        sniHostname == null ? "<none>" : sniHostname,
                        endpoint.tls()));
    }

    /**
     * Indicates if the registry is in closed state.
     * @return true if the registry has been closed.
     */
    public boolean isRegistryClosed() {
        return registryClosed.get();
    }

    /**
     * Signals that the registry should be shut down.  The operation returns a {@link java.util.concurrent.CompletionStage}
     * that will complete once shutdown is complete (endpoints unbound).
     *
     * @return CompletionStage that completes once shut-down is complete.
     */
    public CompletionStage<Void> shutdown() {
        return allOfStage(registeredVirtualClusters.keySet().stream().map(this::deregisterVirtualCluster))
                .whenComplete((u, t) -> {
                    LOGGER.debug("EndpointRegistry shutdown complete.");
                    registryClosed.set(true);
                });
    }

    @Override
    public void close() throws Exception {
        shutdown().toCompletableFuture().get();
    }

    private static <T> CompletableFuture<Void> allOfStage(Stream<CompletionStage<T>> stageStream) {
        return allOfFutures(stageStream.map(CompletionStage::toCompletableFuture));
    }

    private static <T> CompletableFuture<Void> allOfFutures(Stream<CompletableFuture<T>> futureStream) {
        return CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new));
    }
}
