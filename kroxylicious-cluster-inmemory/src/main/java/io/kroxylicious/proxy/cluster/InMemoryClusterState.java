/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.cluster;

import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Objects.requireNonNull;

/**
 * Shared mutable container for proxy cluster state. Thread-safe.
 * Mutations fire registered {@link ProxyClusterListener} callbacks.
 */
public class InMemoryClusterState {

    private volatile Set<ProxyClusterMember> members = Set.of();
    private final Map<String, Integer> vcLeaders = new ConcurrentHashMap<>();
    private final Map<String, Map<String, PartitionAssignment>> vcAssignments = new ConcurrentHashMap<>();
    private final List<ProxyClusterListener> listeners = new CopyOnWriteArrayList<>();

    public Set<ProxyClusterMember> members() {
        return members;
    }

    public void setMembers(Set<ProxyClusterMember> members) {
        requireNonNull(members, "members");
        this.members = Set.copyOf(members);
        for (var listener : listeners) {
            listener.onMembershipChanged(this.members);
        }
    }

    public OptionalInt leaderForVirtualCluster(String virtualClusterName) {
        requireNonNull(virtualClusterName, "virtualClusterName");
        Integer leader = vcLeaders.get(virtualClusterName);
        return leader != null ? OptionalInt.of(leader) : OptionalInt.empty();
    }

    public void setLeaderForVirtualCluster(
                                           String virtualClusterName,
                                           int nodeId) {
        requireNonNull(virtualClusterName, "virtualClusterName");
        vcLeaders.put(virtualClusterName, nodeId);
        for (var listener : listeners) {
            listener.onVirtualClusterLeaderChanged(virtualClusterName, OptionalInt.of(nodeId));
        }
    }

    public PartitionAssignment partitionAssignment(
                                                   String virtualClusterName,
                                                   String namespace) {
        requireNonNull(virtualClusterName, "virtualClusterName");
        requireNonNull(namespace, "namespace");
        var byNamespace = vcAssignments.get(virtualClusterName);
        if (byNamespace == null) {
            return null;
        }
        return byNamespace.get(namespace);
    }

    public void setPartitionAssignment(
                                       String virtualClusterName,
                                       String namespace,
                                       PartitionAssignment assignment) {
        requireNonNull(virtualClusterName, "virtualClusterName");
        requireNonNull(namespace, "namespace");
        requireNonNull(assignment, "assignment");
        vcAssignments.computeIfAbsent(virtualClusterName, k -> new ConcurrentHashMap<>())
                .put(namespace, assignment);
        for (var listener : listeners) {
            listener.onPartitionAssignmentChanged(virtualClusterName, namespace, assignment);
        }
    }

    public void addListener(ProxyClusterListener listener) {
        requireNonNull(listener, "listener");
        listeners.add(listener);
    }

    public void removeListener(ProxyClusterListener listener) {
        requireNonNull(listener, "listener");
        listeners.remove(listener);
    }
}
