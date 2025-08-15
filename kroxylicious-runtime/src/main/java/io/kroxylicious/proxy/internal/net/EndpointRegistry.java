/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.util.Attribute;
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
 *    <li>The registry provides an {@link EndpointBindingResolver}.  The {@link EndpointBindingResolver#resolve(Endpoint, String)} method accepts
 * connection metadata (port, SNI etc) and resolves this to a @{@link BootstrapEndpointBinding}.  This allows
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
 *    <li>updates to the binding mapping are published safely to readers (i.e. threads calling {@link EndpointBindingResolver#resolve(Endpoint, String)}).  This relies the
 *    fact that the binding map uses concurrency safe data structures ({@link ConcurrentHashMap} and the exclusive use of immutable objects within it.</li>
 * </ul>
 */
public class EndpointRegistry implements EndpointReconciler, EndpointBindingResolver, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(EndpointRegistry.class);
    public static final String NO_CHANNEL_BINDINGS_MESSAGE = "No channel bindings found for";
    public static final String VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE = "virtualCluster cannot be null";
    private final NetworkBindingOperationProcessor bindingOperationProcessor;

    interface RoutingKey {
        RoutingKey NULL_ROUTING_KEY = new NullRoutingKey();

        static RoutingKey createBindingKey(@Nullable String sniHostname) {
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
        }
    }

    protected static final AttributeKey<Map<RoutingKey, EndpointBinding>> CHANNEL_BINDINGS = AttributeKey.newInstance("channelBindings");

    private record ReconciliationRecord(Map<Integer, HostPort> upstreamNodeMap, CompletionStage<Void> reconciliationStage) {
        private ReconciliationRecord {
            Objects.requireNonNull(upstreamNodeMap);
            Objects.requireNonNull(reconciliationStage);
        }

        public static ReconciliationRecord createEmptyReconcileRecord() {
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

        public static ListeningChannelRecord create(CompletionStage<Channel> stage) {
            return new ListeningChannelRecord(stage, new AtomicReference<>());
        }
    }

    /** Registry of endpoints and their underlying Netty Channel */
    private final Map<Endpoint, ListeningChannelRecord> listeningChannels = new ConcurrentHashMap<>();

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

        var key = Endpoint.createEndpoint(virtualClusterModel.getBindAddress(), virtualClusterModel.getClusterBootstrapAddress().port(), virtualClusterModel.isUseTls());
        var bootstrapEndpointFuture = registerBinding(key,
                virtualClusterModel.getClusterBootstrapAddress().host(),
                new BootstrapEndpointBinding(virtualClusterModel)).toCompletableFuture();

        vcr.reconciliationRecord().set(ReconciliationRecord.createEmptyReconcileRecord());

        // bind any discovery binding to the bootstrap address
        var discoveryAddressesMapStage = allOfStage(virtualClusterModel.discoveryAddressMap()
                .entrySet()
                .stream()
                .sorted(Map.Entry.comparingByKey()) // ordering not functionality important, but simplifies the unit testing
                .map(e -> {
                    var nodeId = e.getKey();
                    var bhp = e.getValue();
                    return registerBinding(new Endpoint(virtualClusterModel.getBindAddress(), bhp.port(), virtualClusterModel.isUseTls()), bhp.host(),
                            new MetadataDiscoveryBrokerEndpointBinding(virtualClusterModel, nodeId));
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
        LOGGER.warn("Registration error", originalFailure);
        deregisterBinding(virtualClusterModel, vcb -> vcb.endpointGateway().equals(virtualClusterModel))
                .handle((result, throwable) -> {
                    if (throwable != null) {
                        LOGGER.warn("Secondary error occurred whilst handling a previous registration error: {}", originalFailure.getMessage(), throwable);
                    }
                    registeredVirtualClusters.remove(virtualClusterModel);
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
                    deregisterFuture.complete(u);
                }
            });
            return deregisterFuture;
        }

        vcr.registrationStage()
                .thenCompose(u -> deregisterBinding(virtualClusterModel, binding -> binding.endpointGateway().equals(virtualClusterModel))
                        .handle((unused1, t) -> {
                            registeredVirtualClusters.remove(virtualClusterModel);
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
                ReconciliationRecord updated;
                var cand = ReconciliationRecord.createReconcileRecord(upstreamNodes, new CompletableFuture<>());
                if (vcr.reconciliationRecord().compareAndSet(rec, cand)) {
                    // reconcile - work out which bindings are to be registered and which are to be removed.
                    doReconcile(virtualClusterModel, upstreamNodes, cand.reconciliationStage().toCompletableFuture(), vcr);
                    return cand.reconciliationStage();
                }
                else if ((updated = vcr.reconciliationRecord().get()).upstreamNodeMap().equals(upstreamNodes)) {
                    // another thread has since reconciled/started to reconcile the same set of nodes
                    return updated.reconciliationStage();
                }
                else {
                    // another thread has since reconciled to a different set of nodes.
                    return reconcile(virtualClusterModel, upstreamNodes);
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
        var deregs = allOfStage(listeningChannels.values().stream()
                .filter(lcr -> lcr.unbindingStage.get() == null)
                .map(lcr -> lcr.bindingStage()
                        .thenCompose((acceptorChannel -> {
                            var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
                            if (bindings == null || bindings.get() == null) {
                                // nothing to do for this channel
                                return CompletableFuture.completedStage(null);
                            }
                            var bindingMap = bindings.get();

                            return allOfStage(bindingMap.values().stream()
                                    .filter(vcb -> vcb.endpointGateway().equals(virtualClusterModel))
                                    .filter(NodeSpecificEndpointBinding.class::isInstance)
                                    .map(NodeSpecificEndpointBinding.class::cast)
                                    .peek(creations::remove) // side effect
                                    .filter(eb -> !allBrokerIds.contains(eb.nodeId()))
                                    .map(eb -> deregisterBinding(virtualClusterModel, eb::equals)));
                        }))));

        // chain any binding registrations and organise for the reconciliations entry to complete
        deregs.thenCompose(u1 -> allOfStage(creations.stream()
                .map(vcbb -> {
                    var brokerAddress = virtualClusterModel.getBrokerAddress(vcbb.nodeId());
                    var endpoint = Endpoint.createEndpoint(bindingAddress, brokerAddress.port(), virtualClusterModel.isUseTls());
                    return registerBinding(endpoint, brokerAddress.host(), vcbb);
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

    @VisibleForTesting
    boolean isRegistered(EndpointGateway virtualClusterModel) {
        return registeredVirtualClusters.containsKey(virtualClusterModel);
    }

    @VisibleForTesting
    int listeningChannelCount() {
        return listeningChannels.size();
    }

    private CompletionStage<Endpoint> registerBinding(Endpoint key, String host, EndpointBinding virtualClusterBinding) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(virtualClusterBinding, "virtualClusterBinding cannot be null");
        var virtualCluster = virtualClusterBinding.endpointGateway();

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

            bindingOperationProcessor
                    .enqueueNetworkBindingEvent(new NetworkBindRequest(future, Endpoint.createEndpoint(key.bindingAddress(), key.port(), virtualCluster.isUseTls())));
            return r;
        });

        if (lcr.unbindingStage().get() != null) {
            // Listening channel already being unbound, chain this request to the unbind stage, so it completes later.
            return lcr.unbindingStage().get().thenCompose(u -> registerBinding(key, host, virtualClusterBinding));
        }

        return lcr.bindingStage().thenApply(acceptorChannel -> {
            synchronized (lcr) {
                var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
                bindings.setIfAbsent(new ConcurrentHashMap<>());
                var bindingMap = bindings.get();
                var bindingKey = virtualCluster.requiresServerNameIndication() ? RoutingKey.createBindingKey(host) : RoutingKey.NULL_ROUTING_KEY;

                // we use a bindingMap attached to the channel to record the bindings to the channel. the #deregisterBinding path
                // knows to tear down the acceptorChannel when the map becomes empty.
                var existing = bindingMap.putIfAbsent(bindingKey, virtualClusterBinding);

                if (existing instanceof NodeSpecificEndpointBinding existingVcbb && virtualClusterBinding instanceof NodeSpecificEndpointBinding vcbb
                        && existingVcbb.refersToSameVirtualClusterAndNode(vcbb)) {
                    // special case to support update of the upstream target
                    bindingMap.put(bindingKey, virtualClusterBinding);
                }
                else if (existing != null) {
                    throw new EndpointBindingException(
                            "Endpoint %s cannot be bound with key %s binding %s, that key is already bound to %s".formatted(key, bindingKey, virtualClusterBinding,
                                    existing));
                }

                return key;
            }
        });
    }

    private CompletionStage<Void> deregisterBinding(EndpointGateway virtualClusterModel, Predicate<EndpointBinding> predicate) {
        Objects.requireNonNull(virtualClusterModel, VIRTUAL_CLUSTER_CANNOT_BE_NULL_MESSAGE);
        Objects.requireNonNull(predicate, "predicate cannot be null");

        // Search the listening channels for bindings matching the predicate. We could cache more information on the vcr to optimise.
        var unbindStages = listeningChannels.entrySet().stream()
                .map(e -> {
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
                    });
                });

        return allOfStage(unbindStages);
    }

    /**
     * Uses channel metadata (port, SNI name etc.) from the incoming connection to resolve a {@link BootstrapEndpointBinding}.
     *
     * @param endpoint    endpoint being resolved
     * @param sniHostname SNI hostname, may be null.
     * @return completion stage yielding the {@link BootstrapEndpointBinding} or exceptionally a EndpointResolutionException.
     */
    @Override
    public CompletionStage<EndpointBinding> resolve(Endpoint endpoint, @Nullable String sniHostname) {
        var lcr = this.listeningChannels.get(endpoint);
        if (lcr == null || lcr.unbindingStage().get() != null) {
            return CompletableFuture.failedStage(buildEndpointResolutionException("Failed to find channel matching", endpoint, sniHostname));
        }

        return lcr.bindingStage().thenApply(acceptorChannel -> {
            var bindings = acceptorChannel.attr(CHANNEL_BINDINGS);
            if (bindings == null || bindings.get() == null) {
                throw buildEndpointResolutionException(NO_CHANNEL_BINDINGS_MESSAGE, endpoint, sniHostname);
            }
            // We first look for a binding matching by SNI name, then fallback to a null match.
            var binding = bindings.get().getOrDefault(RoutingKey.createBindingKey(sniHostname), bindings.get().get(RoutingKey.NULL_ROUTING_KEY));
            if (binding == null) {
                // If there is an SNI name that matches against the virtual cluster broker address pattern, we generate
                // a restricted broker binding that points at the virtual cluster's bootstrap.
                if (sniHostname != null) {
                    Map<BootstrapEndpointBinding, Integer> bootstrapToBrokerId = findBootstrapBindings(endpoint, sniHostname, bindings);
                    var size = bootstrapToBrokerId.size();
                    if (size > 1) {
                        throw new EndpointResolutionException("Failed to generate an unbound broker binding from SNI " +
                                "as it matches the broker address pattern of more than one virtual cluster",
                                buildEndpointResolutionException(NO_CHANNEL_BINDINGS_MESSAGE, endpoint, sniHostname));
                    }
                    else if (size == 1) {
                        return buildBootstrapBinding(bootstrapToBrokerId);
                    }
                }
                throw buildEndpointResolutionException(NO_CHANNEL_BINDINGS_MESSAGE, endpoint, sniHostname);
            }
            return binding;
        });
    }

    private static EndpointBinding buildBootstrapBinding(Map<BootstrapEndpointBinding, Integer> bootstrapToBrokerId) {
        var e = bootstrapToBrokerId.entrySet().iterator().next();
        var bootstrapBinding = e.getKey();
        var nodeId = e.getValue();
        return new MetadataDiscoveryBrokerEndpointBinding(bootstrapBinding.endpointGateway(), nodeId);
    }

    private HashMap<BootstrapEndpointBinding, Integer> findBootstrapBindings(Endpoint endpoint,
                                                                             String sniHostname,
                                                                             Attribute<Map<RoutingKey, EndpointBinding>> bindings) {
        var allBindingsForPort = bindings.get().values();
        var brokerAddress = new HostPort(sniHostname, endpoint.port());
        var allBootstrapBindings = getAllBootstrapBindings(allBindingsForPort);
        return allBootstrapBindings.stream()
                .collect(HashMap::new, (m, b) -> {
                    var nodeId = b.endpointGateway().getBrokerIdFromBrokerAddress(brokerAddress);
                    if (nodeId != null) {
                        m.put(b, nodeId);
                    }
                }, HashMap::putAll);
    }

    private List<BootstrapEndpointBinding> getAllBootstrapBindings(Collection<EndpointBinding> allBindingsForPort) {
        return allBindingsForPort.stream()
                .filter(BootstrapEndpointBinding.class::isInstance)
                .map(BootstrapEndpointBinding.class::cast)
                .toList();
    }

    private EndpointResolutionException buildEndpointResolutionException(String prefix,
                                                                         Endpoint endpoint,
                                                                         @Nullable String sniHostname) {
        return new EndpointResolutionException(
                ("%s binding address: %s, port: %d, sniHostname: %s, tls: %b").formatted(prefix,
                        endpoint.bindingAddress().orElse("<any>"),
                        endpoint.port(),
                        sniHostname == null ? "<none>" : sniHostname,
                        endpoint.tls()));
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

    private static <T> CompletableFuture<Void> allOfStage(Stream<CompletionStage<T>> stageStream) {
        return allOfFutures(stageStream.map(CompletionStage::toCompletableFuture));
    }

    private static <T> CompletableFuture<Void> allOfFutures(Stream<CompletableFuture<T>> futureStream) {
        return CompletableFuture.allOf(futureStream.toArray(CompletableFuture[]::new));
    }

}
