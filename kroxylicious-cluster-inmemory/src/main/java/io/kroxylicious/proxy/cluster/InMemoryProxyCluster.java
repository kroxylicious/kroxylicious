/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/** In-memory {@link ProxyCluster} backed by a shared {@link InMemoryClusterState}. */
public class InMemoryProxyCluster implements ProxyCluster {

    private final int localNodeId;
    private final InMemoryClusterState state;

    /**
     * Creates a cluster view for the given node, backed by shared state.
     *
     * @param localNodeId the node ID of this instance
     * @param state the shared cluster state
     */
    public InMemoryProxyCluster(
                                int localNodeId,
                                InMemoryClusterState state) {
        if (localNodeId < 1) {
            throw new IllegalArgumentException("localNodeId must be positive, got " + localNodeId);
        }
        this.localNodeId = localNodeId;
        this.state = requireNonNull(state, "state");
    }

    /**
     * Creates a standalone single-node cluster. The node is the sole member,
     * always leader for any virtual cluster queried, and owns all partitions.
     *
     * @param nodeId the node ID
     * @param host the endpoint host
     * @param port the endpoint port
     * @param transport the endpoint transport
     * @return a single-node proxy cluster
     */
    public static InMemoryProxyCluster standalone(
                                                  int nodeId,
                                                  String host,
                                                  int port,
                                                  Transport transport) {
        var endpoint = new ProxyClusterEndpoint(host, port, transport);
        var member = new ProxyClusterMember(nodeId, null, List.of(endpoint));
        var state = new InMemoryClusterState();
        state.setMembers(Set.of(member));
        return new InMemoryProxyCluster(nodeId, state);
    }

    @Override
    public int localNodeId() {
        return localNodeId;
    }

    @Override
    public ProxyClusterMember localMember() {
        return state.members().stream()
                .filter(m -> m.nodeId() == localNodeId)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Local node " + localNodeId + " not found in cluster membership"));
    }

    @Override
    public Set<ProxyClusterMember> members() {
        return state.members();
    }

    @Override
    public OptionalInt leaderForVirtualCluster(String virtualClusterName) {
        return state.leaderForVirtualCluster(virtualClusterName);
    }

    @Override
    public PartitionAssignment partitionAssignment(
                                                   String virtualClusterName,
                                                   String namespace) {
        return state.partitionAssignment(virtualClusterName, namespace);
    }

    @Override
    public void addListener(ProxyClusterListener listener) {
        state.addListener(listener);
    }

    @Override
    public void removeListener(ProxyClusterListener listener) {
        state.removeListener(listener);
    }
}
