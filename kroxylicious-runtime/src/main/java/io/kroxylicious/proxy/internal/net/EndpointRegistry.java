/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

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
 *    <li>The registry does not take direct responsibility for binding and unbinding network sockets.  Instead, it emits
 * network binding operations {@link NetworkBindingOperation} which are processed by a {@link NetworkBindingOperationProcessor}.
 *    <li>The registry exposes methods for the registration {@link #registerVirtualCluster(EndpointGateway)} and deregistration
 * {@link #deregisterVirtualCluster(EndpointGateway)} of virtual clusters.  The registry emits the required network binding
 * operations to expose the virtual cluster to the network.  These API calls return futures that will complete once
 * the underlying network operations are completed.</li>
 *    <li>The registry provides an {@link EndpointBindingResolver}.  The {@link EndpointBindingResolver#resolve(Channel, String)} method accepts
 * a child channel and resolves it to an {@link EndpointBinding} via the acceptor channel's bindings.  This allows
 * Kroxylicious to determine the destination of any incoming connection.</li>
 *    <li>The registry provides a {@link EndpointReconciler}. The {@link EndpointReconciler#reconcile(EndpointGateway, Map)} method accepts a map describing
 *    the target cluster's broker topology.  The job of the reconciler is to make adjustments to the network bindings (binding/unbinding ports) to
 *    fully expose the brokers of the target cluster through the virtual cluster.</li>
 * </ul>
 * The registry is thread safe for all operations.
 * <ul>
 *    <li>virtual cluster registration uses java.util.concurrent features to ensure registration is single threaded.</li>
 *    <li>virtual cluster de-registration uses java.util.concurrent.atomic to ensure de-registration is single threaded.</li>
 *    <li>virtual cluster reconciliation uses java.util.concurrent.atomic to ensure reconciliation is single threaded.</li>
 *    <li>updates to the binding mapping (attached to channel) are made only whilst holding an intrinsic lock on the {@link ListeningChannelRecord}.</li>
 *    <li>updates to the binding mapping are published safely to readers (i.e. threads calling {@link EndpointBindingResolver#resolve(Channel, String)}).  This relies the
 *    fact that the binding map uses concurrency safe data structures ({@link ConcurrentHashMap} and the exclusive use of immutable objects within it.</li>
 * </ul>
 */
public class EndpointRegistry implements EndpointReconciler, EndpointBindingResolver, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointRegistry.class);
    public static final String NO_CHANNEL_BINDINGS_MESSAGE = "No channel bindings found for channel";
    public static final String VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE = "virtualCluster cannot be null";

    private final NetworkBindingOperationProcessor bindingOperationProcessor;

    /**
     * Allocates synthetic, distinct channel keys for OS-assigned (port 0) ports on gateways that
     * don't require SNI. Each such binding needs its own acceptor channel, since the OS hasn't
     * assigned a real port yet and port 0 can't itself serve as a distinguishing map key.
     */
    private static final AtomicInteger SYNTHETIC_PORT_COUNTER = new AtomicInteger(-1);

    protected static final AttributeKey<Map<ProxyNodeId, EndpointBinding>> CHANNEL_BINDINGS = AttributeKey.newInstance("channelBindings");

    private record ReconciliationRecord(Map<Integer, HostPort> upstreamNodeMap, CompletionStage<Void> reconciliationStage) {
        private ReconciliationRecord {
            Objects.requireNonNull(upstreamNodeMap);
            Objects.requireNonNull(reconciliationStage);
        }

        private static ReconciliationRecord createEmptyReconcileRecord() {
            return ReconciliationRecord.createReconcileRecord(Map.of(), CompletableFuture.completedStage(null));
        }

        private static ReconciliationRecord createReconcileRecord(Map<Integer, HostPort> upstreamNodeMap, CompletionStage<Void> future) {
            return new ReconciliationRecord(upstreamNodeMap, future);
        }

    }

    private record VirtualClusterRecord(CompletionStage<Endpoint> registrationStage, AtomicReference<ReconciliationRecord> reconciliationRecord,
                                        AtomicReference<CompletionStage<Void>> deregistrationStage) {
        private VirtualClusterRecord {
            Objects.requireNonNull(registrationStage);
            Objects.requireNonNull(reconciliationRecord);
            Objects.requireNonNull(deregistrationStage);
        }

        private static VirtualClusterRecord create(CompletionStage<Endpoint> stage) {
            return new VirtualClusterRecord(stage, new AtomicReference<>(), new AtomicReference<>());
        }
    }

    /** Registry of virtual clusters that have been registered */
    private final Map<EndpointGateway, VirtualClusterRecord> registeredVirtualClusters = new ConcurrentHashMap<>();

    private record ListeningChannelRecord(CompletionStage<Channel> bindingStage, AtomicReference<CompletionStage<Void>> unbindingStage) {
        private ListeningChannelRecord {
            Objects.requireNonNull(bindingStage);
            Objects.requireNonNull(unbindingStage);
        }

        private static ListeningChannelRecord create(CompletionStage<Channel> stage) {
            return new ListeningChannelRecord(stage, new AtomicReference<>());
        }
    }

    /** Registry of endpoints and their underlying Netty Channel */
    private final Map<Endpoint, ListeningChannelRecord> listeningChannels = new ConcurrentHashMap<>();

    /**
     * Maps each virtual node to its {@link ListeningChannelRecord}, allowing
     * {@link #resolvePort(ProxyNodeId)} to read the actual bound port directly
     * from the channel (important when port=0 / OS-assigned was configured).
     */
    private final Map<ProxyNodeId, ListeningChannelRecord> virtualNodeIndex = new ConcurrentHashMap<>();

    public EndpointRegistry(NetworkBindingOperationProcessor bindingOperationProcessor) {
        this.bindingOperationProcessor = bindingOperationProcessor;
    }

    /**
     * Registers a virtual cluster with the registry.  The registry refers to the endpoint configuration
     * of the virtual cluster to understand its port requirements, binding new listening ports as necessary.
     * This operation returns a {@link CompletionStage<Endpoint>}.  The completion stage will complete once
     * all necessary network bind operations are completed.  The returned {@link Endpoint} refers to the
     * bootstrap endpoint of the virtual cluster.
     *
     * @param virtualClusterModel virtual cluster to be registered.
     * @return completion stage that will complete after registration is finished.
     */
    public CompletionStage<Endpoint> registerVirtualCluster(EndpointGateway virtualClusterModel) {
        Objects.requireNonNull(virtualClusterModel, VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE);

        var vcr = VirtualClusterRecord.create(new CompletableFuture<>());
        var current = registeredVirtualClusters.putIfAbsent(virtualClusterModel, vcr);
        if (current != null) {
            var deregistration = current.deregistrationStage().get();
            if (deregistration != null) {
                // the cluster is already being deregistered, so perform this work after it completes.
                return deregistration.thenCompose(u -> registerVirtualCluster(virtualClusterModel));
            }
            return current.registrationStage();
        }

        var bindingSpec = virtualClusterModel.bindingSpec();
        var bootstrapBindAddress = bindingSpec.getBootstrapBindAddress();
        var key = Endpoint.createEndpoint(bindingSpec.getBindAddress(), bootstrapBindAddress.port(), virtualClusterModel.isUseTls());
        var bootstrapEndpointFuture = registerBinding(key,
                new BootstrapEndpointBinding(virtualClusterModel),
                new ProxyNodeId.Bootstrap(virtualClusterModel)).toCompletableFuture();

        vcr.reconciliationRecord().set(ReconciliationRecord.createEmptyReconcileRecord());

        // bind any discovery binding
        var discoveryAddressesMapStage = allOfStage(bindingSpec.nodeBindAddresses()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey()) // ordering not functionality important, but simplifies the unit testing
                .map(e -> {
                    var nodeId = e.getKey();
                    var bhp = e.getValue();
                    var discoveryEndpoint = new Endpoint(bindingSpec.getBindAddress(), bhp.port(), virtualClusterModel.isUseTls());
                    return registerBinding(discoveryEndpoint,
                            new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel, nodeId),
                            new ProxyNodeId.Broker(virtualClusterModel, nodeId));
                }));

        bootstrapEndpointFuture.thenCombine(discoveryAddressesMapStage, (bef, bps) -> bef)
                .whenComplete((u, t) -> {
                    var future = vcr.registrationStage.toCompletableFuture();
                    if (t != null) {
                        rollbackRelatedBindings(virtualClusterModel, t, future);
                    }
                    else {
                        handleSuccessfulBinding(bootstrapEndpointFuture, future);
                    }
                });

        return vcr.registrationStage();
    }

    private static void handleSuccessfulBinding(CompletableFuture<Endpoint> bootstrapEndpointFuture, CompletableFuture<Endpoint> future) {
        try {
            future.complete(bootstrapEndpointFuture.get());
        }
        catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            future.completeExceptionally(ie);
        }
        catch (ExecutionException ee) {
            future.completeExceptionally(ee.getCause());
        }
        catch (RuntimeException runtimeException) {
            future.completeExceptionally(runtimeException);
        }
    }

    /**
     * Try to roll back any bindings that were successfully made
     */
    private void rollbackRelatedBindings(EndpointGateway virtualClusterModel, Throwable originalFailure, CompletableFuture<Endpoint> future) {
        LOGGER.atWarn()
                .setCause(originalFailure)
                .log("Registration error");
        deregisterBinding(virtualClusterModel, vcb -> vcb.endpointGateway().equals(virtualClusterModel))
                .handle((result, throwable) -> {
                    if (throwable != null) {
                        LOGGER.atWarn()
                                .setCause(throwable)
                                .addKeyValue("originalError", originalFailure.getMessage())
                                .log("Secondary error occurred whilst handling a previous registration error");
                    }
                    registeredVirtualClusters.remove(virtualClusterModel);
                    removeVirtualNodeEntries(virtualClusterModel);
                    future.completeExceptionally(originalFailure);
                    return null;
                });
    }

    /**
     * De-registers a virtual cluster from the registry, removing all existing endpoint bindings, and
     * closing any listening sockets that are no longer required. This operation returns a {@link CompletionStage<Endpoint>}.
     * The completion stage will complete once any necessary network unbind operations are completed.
     *
     * @param virtualClusterModel virtual cluster to be deregistered.
     * @return completion stage that will complete after registration is finished.
     */
    public CompletionStage<Void> deregisterVirtualCluster(EndpointGateway virtualClusterModel) {
        Objects.requireNonNull(virtualClusterModel, VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE);

        var vcr = registeredVirtualClusters.get(virtualClusterModel);
        if (vcr == null) {
            // cluster not currently registered
            return CompletableFuture.completedFuture(null);
        }

        var deregisterFuture = new CompletableFuture<Void>();
        var updated = vcr.deregistrationStage().compareAndSet(null, deregisterFuture);
        if (!updated) {
            // cluster de-registration already in progress.
            vcr.deregistrationStage().get().whenComplete((u, t) -> {
                if (t != null) {
                    deregisterFuture.completeExceptionally(t);
                }
                else {
                    deregisterFuture.complete(null);
                }
            });
            return deregisterFuture;
        }

        vcr.registrationStage()
                .thenCompose(u -> deregisterBinding(virtualClusterModel, binding -> binding.endpointGateway().equals(virtualClusterModel))
                        .handle((unused1, t) -> {
                            registeredVirtualClusters.remove(virtualClusterModel);
                            removeVirtualNodeEntries(virtualClusterModel);
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

    /**
     * Reconciles the current set of bindings for this virtual cluster against those required by the current set of nodes.
     *
     * @param virtualClusterModel virtual cluster
     * @param upstreamNodes  current map of node id to upstream host ports
     * @return CompletionStage yielding true if binding alterations were made, or false otherwise.
     */
    @Override
    public CompletionStage<Void> reconcile(EndpointGateway virtualClusterModel, Map<Integer, HostPort> upstreamNodes) {
        Objects.requireNonNull(virtualClusterModel, VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE);
        Objects.requireNonNull(upstreamNodes, "upstreamNodes cannot be null");

        var vcr = registeredVirtualClusters.get(virtualClusterModel);
        if (vcr == null) {
            // cluster not currently registered
            return CompletableFuture.failedStage(new IllegalStateException("virtual cluster %s not registered or is being deregistered".formatted(virtualClusterModel)));
        }
        else if (vcr.deregistrationStage().get() != null) {
            // cluster in process of being deregistered, ignore the reconciliation request
            return CompletableFuture.completedStage(null);
        }

        // TODO: consider composing all of this after registration has finished

        var rec = vcr.reconciliationRecord().get();
        if (rec == null) {
            // Should never happen.
            return CompletableFuture.failedStage(new IllegalStateException("virtual cluster %s in unexpected state".formatted(virtualClusterModel)));
        }

        if (rec.upstreamNodeMap().equals(upstreamNodes)) {
            // set of nodeIds already reconciled or reconciliation of those nodes ids is already in progress.
            return rec.reconciliationStage();
        }
        else {
            // set of nodeIds differ
            return rec.reconciliationStage().thenCompose(u -> {
                var cand = ReconciliationRecord.createReconcileRecord(upstreamNodes, new CompletableFuture<>());
                if (vcr.reconciliationRecord().compareAndSet(rec, cand)) {
                    // reconcile - work out which bindings are to be registered and which are to be removed.
                    doReconcile(virtualClusterModel, upstreamNodes, cand.reconciliationStage().toCompletableFuture(), vcr);
                    return cand.reconciliationStage();
                }
                else {
                    ReconciliationRecord updated = vcr.reconciliationRecord().get();
                    if (updated.upstreamNodeMap().equals(upstreamNodes)) {
                        // another thread has since reconciled/started to reconcile the same set of nodes
                        return updated.reconciliationStage();
                    }
                    else {
                        // another thread has since reconciled to a different set of nodes.
                        return reconcile(virtualClusterModel, upstreamNodes);
                    }
                }
            });
        }
    }

    private void doReconcile(EndpointGateway virtualClusterModel, Map<Integer, HostPort> upstreamNodes, CompletableFuture<Void> future, VirtualClusterRecord vcr) {
        var bindingAddress = virtualClusterModel.getBindAddress();

        var discoveryBrokerIds = Optional.ofNullable(virtualClusterModel.discoveryAddressMap()).map(Map::keySet).orElse(Set.of());
        var allBrokerIds = Stream.concat(discoveryBrokerIds.stream(), upstreamNodes.keySet().stream()).collect(Collectors.toUnmodifiableSet());

        var creations = constructPossibleBindingsToCreate(virtualClusterModel, upstreamNodes);

        // first assemble the stream of de-registrations (and by side effect: update creations)
        var deregs = allOfStage(allChannelRecords()
                .filter(lcr -> lcr.unbindingStage.get() == null)
                .map(lcr -> lcr.bindingStage()
                        .thenCompose(acceptorChannel -> {
                            var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
                            if (bindings == null || bindings.get() == null) {
                                // nothing to do for this channel
                                return CompletableFuture.completedStage(null);
                            }
                            var bindingMap = bindings.get();

                            List<NodeSpecificEndpointBinding> nodeSpecificBindings = bindingMap.values().stream()
                                    .filter(vcb -> vcb.endpointGateway().equals(virtualClusterModel))
                                    .filter(NodeSpecificEndpointBinding.class::isInstance)
                                    .map(NodeSpecificEndpointBinding.class::cast).toList();
                            nodeSpecificBindings.forEach(creations::remove);
                            return allOfStage(nodeSpecificBindings.stream()
                                    .filter(eb -> !allBrokerIds.contains(eb.nodeId()))
                                    .map(eb -> {
                                        virtualNodeIndex.remove(new ProxyNodeId.Broker(virtualClusterModel, eb.nodeId()));
                                        return deregisterBinding(virtualClusterModel, eb::equals);
                                    }));
                        })));

        // chain any binding registrations and organise for the reconciliations entry to complete
        deregs.thenCompose(u1 -> allOfStage(creations.stream()
                .map(vcbb -> {
                    var brokerAddress = virtualClusterModel.getBrokerAddress(vcbb.nodeId());
                    var endpoint = Endpoint.createEndpoint(bindingAddress, brokerAddress.port(), virtualClusterModel.isUseTls());
                    return registerBinding(endpoint, vcbb,
                            new ProxyNodeId.Broker(virtualClusterModel, vcbb.nodeId()));
                })))
                .whenComplete((u2, t) -> {
                    if (t != null) {
                        // if the reconciliation fails, we want to retry the next time a caller reconciles the same set of brokers.
                        vcr.reconciliationRecord().set(ReconciliationRecord.createEmptyReconcileRecord());
                        future.completeExceptionally(t);

                    }
                    else {
                        future.complete(null);
                    }
                });
    }

    private Set<EndpointBinding> constructPossibleBindingsToCreate(EndpointGateway virtualClusterModel,
                                                                   Map<Integer, HostPort> upstreamNodes) {
        var discoveryBrokerIds = virtualClusterModel.discoveryAddressMap();
        // create possible set of bindings to create
        var creations = upstreamNodes.entrySet()
                .stream()
                .map(e -> new BrokerEndpointBinding(virtualClusterModel, e.getValue(), e.getKey()))
                .map(EndpointBinding.class::cast)
                .collect(Collectors.toCollection(ConcurrentHashMap::newKeySet));
        // add bindings corresponding to any pre-bindings. There are marked as restricted and point to bootstrap.
        creations.addAll(discoveryBrokerIds
                .keySet()
                .stream()
                .filter(Predicate.not(upstreamNodes::containsKey))
                .map(nodeId -> new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel, nodeId))
                .toList());
        return creations;
    }

    @Override
    public Optional<HostPort> upstreamAddress(EndpointGateway gateway, int upstreamNodeId) {
        return Optional.ofNullable(registeredVirtualClusters.get(gateway))
                .map(vcr -> vcr.reconciliationRecord().get())
                .map(rec -> rec.upstreamNodeMap().get(upstreamNodeId));
    }

    @VisibleForTesting
    boolean isRegistered(EndpointGateway virtualClusterModel) {
        return registeredVirtualClusters.containsKey(virtualClusterModel);
    }

    @VisibleForTesting
    int listeningChannelCount() {
        return listeningChannels.size();
    }

    /**
     * Returns the actual port the OS bound for a given virtual node.
     * <p>
     * For nodes configured with an explicit port this is the same as the configured port.
     * For nodes configured with port=0 (OS-assigned), this returns the ephemeral port
     * the OS selected at bind time.
     *
     * @param vn the virtual node whose bound port is needed
     * @return a stage that completes with the actual bound port once the binding is established
     * @throws IllegalStateException if the virtual node has no recorded binding (not yet registered)
     */
    public CompletionStage<Integer> resolvePort(ProxyNodeId vn) {
        var record = virtualNodeIndex.get(vn);
        if (record == null) {
            throw new IllegalStateException("No binding found for virtual node " + vn);
        }
        return record.bindingStage().thenApply(ch -> requireInetSocketAddress(ch).getPort());
    }

    private void removeVirtualNodeEntries(EndpointGateway gateway) {
        virtualNodeIndex.entrySet().removeIf(e -> e.getKey().gateway().equals(gateway));
    }

    /**
     * Returns the key to use in the {@link #listeningChannels} map for the given configured endpoint.
     * This controls whether bindings share an acceptor channel: bindings that return the same key
     * share one channel. Gateways requiring SNI always share a channel keyed by the configured
     * endpoint (disambiguation happens later, via {@link ChannelAddressingSpec}). Gateways that
     * don't require SNI get their own channel per OS-assigned (port 0) binding, since there's no
     * other way to distinguish them once bound.
     */
    private static Endpoint channelKeyFor(Endpoint configured, boolean requiresServerNameIndication) {
        if (requiresServerNameIndication || configured.port() != 0) {
            return configured;
        }
        return Endpoint.createEndpoint(configured.bindingAddress(), SYNTHETIC_PORT_COUNTER.getAndDecrement(), configured.tls());
    }

    // ListeningChannelRecord instances are stable shared objects from the listeningChannels map, intentionally used as per-channel locks
    @SuppressWarnings("java:S2445")
    private CompletionStage<Endpoint> registerBinding(Endpoint key, EndpointBinding virtualClusterBinding, ProxyNodeId nodeId) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(virtualClusterBinding, "virtualClusterBinding cannot be null");
        var virtualCluster = virtualClusterBinding.endpointGateway();

        var channelKey = channelKeyFor(key, virtualCluster.requiresServerNameIndication());
        var lcr = listeningChannels.computeIfAbsent(channelKey, ck -> {
            var future = new CompletableFuture<Channel>();
            var r = ListeningChannelRecord.create(future.exceptionally(t -> {
                listeningChannels.remove(ck);
                if (t instanceof RuntimeException re) {
                    throw re;
                }
                else {
                    throw new RuntimeException(t);
                }
            }));

            bindingOperationProcessor
                    .enqueueNetworkBindingEvent(new NetworkBindRequest(future, Endpoint.createEndpoint(key.bindingAddress(), key.port(), virtualCluster.isUseTls())));
            return r;
        });

        if (lcr.unbindingStage().get() != null) {
            // Listening channel already being unbound, chain this request to the unbind stage, so it completes later.
            return lcr.unbindingStage().get().thenCompose(u -> registerBinding(key, virtualClusterBinding, nodeId));
        }

        return lcr.bindingStage().thenApply(acceptorChannel -> {
            virtualNodeIndex.put(nodeId, lcr);
            notifyBoundPort(nodeId, acceptorChannel);
            synchronized (lcr) {
                var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
                bindings.setIfAbsent(new ConcurrentHashMap<>());

                var bindingMap = bindings.get();

                // a channel not requiring SNI cannot disambiguate connections beyond the port they
                // arrived on, so at most one gateway may ever occupy it.
                if (!virtualCluster.requiresServerNameIndication()) {
                    var foreignBinding = bindingMap.values().stream()
                            .filter(b -> !b.endpointGateway().equals(virtualCluster))
                            .findAny();
                    if (foreignBinding.isPresent()) {
                        throw new EndpointBindingException(
                                "Endpoint %s cannot be bound with key %s binding %s, that key is already bound to %s".formatted(key, nodeId, virtualClusterBinding,
                                        foreignBinding.get()));
                    }
                }

                // we use a bindingMap attached to the channel to record the bindings to the channel. the #deregisterBinding path
                // knows to tear down the acceptorChannel when the map becomes empty.
                var existing = bindingMap.putIfAbsent(nodeId, virtualClusterBinding);

                if (existing instanceof NodeSpecificEndpointBinding existingVcbb && virtualClusterBinding instanceof NodeSpecificEndpointBinding vcbb
                        && existingVcbb.refersToSameVirtualClusterAndNode(vcbb)) {
                    // special case to support update of the upstream target
                    bindingMap.put(nodeId, virtualClusterBinding);
                }
                else if (existing != null) {
                    throw new EndpointBindingException(
                            "Endpoint %s cannot be bound with key %s binding %s, that key is already bound to %s".formatted(key, nodeId, virtualClusterBinding,
                                    existing));
                }

                return key;
            }
        });
    }

    private static void notifyBoundPort(ProxyNodeId nodeId, Channel acceptorChannel) {
        int actualPort = requireInetSocketAddress(acceptorChannel).getPort();
        switch (nodeId) {
            case ProxyNodeId.Broker(var gateway, int brokerNodeId) -> gateway.bindingSpec().registerBoundPort(brokerNodeId, actualPort);
            case ProxyNodeId.Bootstrap(var gateway) -> gateway.bindingSpec().registerBoundBootstrapPort(actualPort);
        }
    }

    @SuppressWarnings("java:S2445") // see registerBinding
    private CompletionStage<Void> deregisterBinding(EndpointGateway virtualClusterModel, Predicate<EndpointBinding> predicate) {
        Objects.requireNonNull(virtualClusterModel, VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE);
        Objects.requireNonNull(predicate, "predicate cannot be null");

        var unbindStages = allChannelRecords()
                .map(lcr -> lcr.bindingStage().thenCompose(acceptorChannel -> {
                    synchronized (lcr) {
                        var bindingMap = acceptorChannel.attr(EndpointRegistry.CHANNEL_BINDINGS).get();
                        var allEntries = bindingMap.entrySet();
                        var toRemove = allEntries.stream().filter(be -> predicate.test(be.getValue())).collect(Collectors.toSet());
                        // If our removal leaves the channel without bindings, trigger its unbinding
                        if (allEntries.removeAll(toRemove) && bindingMap.isEmpty()) {
                            var unbindFuture = new CompletableFuture<Void>();
                            var afterUnbind = unbindFuture.whenComplete((u, t) -> listeningChannels.values().remove(lcr));
                            if (lcr.unbindingStage().compareAndSet(null, afterUnbind)) {
                                bindingOperationProcessor
                                        .enqueueNetworkBindingEvent(new NetworkUnbindRequest(virtualClusterModel.isUseTls(), acceptorChannel, unbindFuture));
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
                }));

        return allOfStage(unbindStages);
    }

    private Stream<ListeningChannelRecord> allChannelRecords() {
        return listeningChannels.values().stream();
    }

    /**
     * Uses the acceptor channel (parent of the incoming connection) to resolve an {@link EndpointBinding}.
     * The acceptor channel carries the bindings registered at bind time via the {@link #CHANNEL_BINDINGS}
     * attribute. The candidate gateways sharing the channel are asked, via {@link ChannelAddressingSpec},
     * to identify the connection; the resolved identity is then looked up (or, for a not-yet-reconciled
     * broker, synthesized) against the registered bindings.
     *
     * @param channel     the child channel representing the accepted connection
     * @param sniHostname SNI hostname, may be null.
     * @return completion stage yielding the {@link EndpointBinding} or exceptionally a EndpointResolutionException.
     */
    @Override
    public CompletionStage<EndpointBinding> resolve(Channel channel, @Nullable String sniHostname) {
        var acceptorChannel = channel.parent();
        if (acceptorChannel == null) {
            return CompletableFuture.failedStage(
                    new EndpointResolutionException("Channel has no parent (acceptor): %s".formatted(channel)));
        }

        var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
        if (bindings == null || bindings.get() == null) {
            return CompletableFuture.failedStage(
                    new EndpointResolutionException(NO_CHANNEL_BINDINGS_MESSAGE + " %s".formatted(acceptorChannel)));
        }

        var bindingMap = bindings.get();
        var acceptorPort = requireInetSocketAddress(acceptorChannel).getPort();

        var candidateGateways = bindingMap.values().stream().map(EndpointBinding::endpointGateway).distinct().toList();
        var match = new ChannelAddressingSpec(candidateGateways).identify(acceptorPort, sniHostname);
        if (match.isEmpty()) {
            return CompletableFuture.failedStage(
                    new EndpointResolutionException(NO_CHANNEL_BINDINGS_MESSAGE + " channel %s sniHostname %s".formatted(acceptorChannel, sniHostname)));
        }

        var gateway = match.get().gateway();
        EndpointBinding binding = switch (match.get().target()) {
            case AddressingSpec.Target.Bootstrap ignored -> bindingMap.get(new ProxyNodeId.Bootstrap(gateway));
            case AddressingSpec.Target.Node(int nodeId) -> {
                var existing = bindingMap.get(new ProxyNodeId.Broker(gateway, nodeId));
                yield existing != null ? existing : new MetadataDiscoveryBrokerEndpointBinding(gateway, nodeId);
            }
            case AddressingSpec.Target.NotRecognised ignored -> throw new IllegalStateException(
                    "ChannelAddressingSpec matched a candidate with target NotRecognised, which should be impossible");
        };

        if (binding == null) {
            return CompletableFuture.failedStage(
                    new EndpointResolutionException(NO_CHANNEL_BINDINGS_MESSAGE + " channel %s sniHostname %s".formatted(acceptorChannel, sniHostname)));
        }
        return CompletableFuture.completedStage(binding);
    }

    /**
     * Signals that the registry should be shut down.  The operation returns a {@link java.util.concurrent.CompletionStage}
     * that will complete once shutdown is complete (endpoints unbound).
     *
     * @return CompletionStage that completes once shut-down is complete.
     */
    public CompletionStage<Void> shutdown() {
        return allOfStage(registeredVirtualClusters.keySet().stream().map(this::deregisterVirtualCluster));
    }

    @Override
    public void close() throws Exception {
        shutdown().toCompletableFuture().get();
    }

    private static InetSocketAddress requireInetSocketAddress(Channel channel) {
        if (!(channel.localAddress() instanceof InetSocketAddress inet)) {
            throw new IllegalStateException("Expected InetSocketAddress but got " + channel.localAddress());
        }
        return inet;
    }

    private static <T> CompletableFuture<Void> allOfStage(Stream<CompletionStage<T>> stageStream) {
        return allOfFutures(stageStream.map(CompletionStage::toCompletableFuture));
    }

    private static <T> CompletableFuture<Void> allOfFutures(Stream<CompletableFuture<T>> futureStream) {
        return CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new));
    }

}
