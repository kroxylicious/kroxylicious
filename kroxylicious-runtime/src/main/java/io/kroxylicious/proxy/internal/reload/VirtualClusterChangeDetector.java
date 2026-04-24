/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;

/**
 * Identifies virtual clusters that were added, removed, or modified by comparing the
 * {@link VirtualCluster} config records in the old and new {@link io.kroxylicious.proxy.config.Configuration}.
 * <p>
 * A cluster is considered <em>modified</em> when any of its components differs between old and
 * new. Most components are compared via {@link Objects#equals} (which on records is a canonical
 * deep compare). The {@code gateways} list is compared with {@link KeyedListEquality} so that
 * reordering gateway entries in YAML — a semantic no-op — does not trigger a spurious restart.
 * <p>
 * <b>Design note:</b> the hot-reload proposal describes this detector as comparing
 * {@code VirtualClusterModel} instances via {@code equals()}. In practice the model class has
 * no {@code equals()} override and carries derived runtime state (SSL contexts, etc.) that
 * isn't suitable for value-level comparison. The config record is the source of truth for
 * "what changed between deployments" and is the correct carrier for change detection.
 */
public class VirtualClusterChangeDetector implements ChangeDetector {

    @Override
    public ChangeResult detect(ConfigurationChangeContext context) {
        Map<String, VirtualCluster> oldByName = indexByName(context.oldConfig().virtualClusters());
        Map<String, VirtualCluster> newByName = indexByName(context.newConfig().virtualClusters());

        Set<String> toAdd = new HashSet<>(newByName.keySet());
        toAdd.removeAll(oldByName.keySet());

        Set<String> toRemove = new HashSet<>(oldByName.keySet());
        toRemove.removeAll(newByName.keySet());

        Set<String> toModify = new HashSet<>();
        for (Map.Entry<String, VirtualCluster> entry : oldByName.entrySet()) {
            String name = entry.getKey();
            VirtualCluster newCluster = newByName.get(name);
            if (newCluster != null && isModified(entry.getValue(), newCluster)) {
                toModify.add(name);
            }
        }

        return new ChangeResult(toAdd, toRemove, toModify);
    }

    /**
     * Field-by-field comparison that knows which components are semantically ordered and which
     * aren't. We don't simply call {@code old.equals(newer)} because the record's auto-generated
     * {@code equals} treats {@code gateways} as an ordered list, producing false positives when
     * a user reorders gateway entries without changing the set of gateways.
     */
    private static boolean isModified(VirtualCluster old, VirtualCluster newer) {
        return !Objects.equals(old.name(), newer.name())
                || !Objects.equals(old.targetCluster(), newer.targetCluster())
                || old.logNetwork() != newer.logNetwork()
                || old.logFrames() != newer.logFrames()
                // Filter chain order is semantically meaningful; keep ordered compare.
                || !Objects.equals(old.filters(), newer.filters())
                || !Objects.equals(old.subjectBuilder(), newer.subjectBuilder())
                || !Objects.equals(old.topicNameCache(), newer.topicNameCache())
                // Gateways are a name-keyed semantic set; compare via KeyedListEquality.
                || !KeyedListEquality.equal(old.gateways(), newer.gateways(), VirtualClusterGateway::name);
    }

    private static Map<String, VirtualCluster> indexByName(List<VirtualCluster> clusters) {
        return clusters.stream().collect(Collectors.toMap(VirtualCluster::name, Function.identity()));
    }
}
