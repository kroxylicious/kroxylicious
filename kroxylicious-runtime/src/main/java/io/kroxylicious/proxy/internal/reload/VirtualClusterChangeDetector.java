/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.VirtualCluster;

/**
 * Identifies virtual clusters that were added, removed, or modified by comparing the
 * {@link VirtualCluster} config records in the old and new {@link io.kroxylicious.proxy.config.Configuration}.
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
            if (newCluster != null && !entry.getValue().sameAs(newCluster)) {
                toModify.add(name);
            }
        }

        return new ChangeResult(toAdd, toRemove, toModify);
    }

    private static Map<String, VirtualCluster> indexByName(List<VirtualCluster> clusters) {
        return clusters.stream().collect(Collectors.toMap(VirtualCluster::name, Function.identity()));
    }
}
