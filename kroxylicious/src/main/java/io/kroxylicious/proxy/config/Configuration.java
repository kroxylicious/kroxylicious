/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;

public record Configuration(AdminHttpConfiguration adminHttp,
                            Map<String, VirtualCluster> virtualClusters,
                            List<FilterDefinition> filters,
                            List<MicrometerDefinition> micrometer,
                            boolean useIoUring) {
    public AdminHttpConfiguration adminHttpConfig() {
        return adminHttp();
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer() == null ? List.of() : micrometer();
    }

    public boolean isUseIoUring() {
        return useIoUring();
    }

    public List<io.kroxylicious.proxy.model.VirtualCluster> validClusters() {

        final List<io.kroxylicious.proxy.model.VirtualCluster> modelClusters = virtualClusters.entrySet().stream()
                .map(entry -> entry.getValue().toVirtualClusterModel(entry.getKey()))
                .toList();
        final Map<String, Integer> clusterNameCounts = new HashMap<>();
        modelClusters.stream().map(io.kroxylicious.proxy.model.VirtualCluster::getClusterName).forEach(clusterName -> {
            final Integer appearanceCount = clusterNameCounts.computeIfAbsent(clusterName, ignored -> 0) + 1;
            clusterNameCounts.put(clusterName, appearanceCount);
        });

        if (clusterNameCounts.entrySet().stream().anyMatch(entry -> entry.getValue() >= 2)) {
            return modelClusters.stream().filter(Predicate.not(virtualCluster -> clusterNameCounts.get(virtualCluster.getClusterName()) >= 2)).toList();
        }
        else {
            return modelClusters;
        }
    }
}
