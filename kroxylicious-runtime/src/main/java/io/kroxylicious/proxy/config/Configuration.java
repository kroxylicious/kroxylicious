/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;

import io.kroxylicious.proxy.config.admin.AdminHttpConfiguration;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public record Configuration(
        @Nullable
        AdminHttpConfiguration adminHttp,
        Map<String, VirtualCluster> virtualClusters,
        List<FilterDefinition> filters,
        List<MicrometerDefinition> micrometer,
        boolean useIoUring
) {
    public @Nullable AdminHttpConfiguration adminHttpConfig() {
        return adminHttp();
    }

    public List<MicrometerDefinition> getMicrometer() {
        return micrometer() == null ? List.of() : micrometer();
    }

    public boolean isUseIoUring() {
        return useIoUring();
    }

    public @NonNull List<io.kroxylicious.proxy.model.VirtualCluster> virtualClusterModel() {
        return virtualClusters.entrySet()
                              .stream()
                              .map(entry -> entry.getValue().toVirtualClusterModel(entry.getKey()))
                              .toList();
    }
}
