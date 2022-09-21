/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.testcluster;

import java.util.AbstractMap.SimpleEntry;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterFactory.class);

    /**
     * provides default inVM setting for callers that do not provide a inVM.
     */
    public static final String TEST_CLUSTER_IN_VM_DEFAULT = "TEST_CLUSTER_IN_VM";

    /**
     * provides default kraftMode setting for callers that do not provide a KraftMode.
     */
    public static final String TEST_CLUSTER_KRAFT_MODE_DEFAULT = "TEST_CLUSTER_KRAFT_MODE";

    public static Cluster create(ClusterConfig clusterConfig) {
        if (clusterConfig == null) {
            throw new NullPointerException();
        }

        var builder = clusterConfig.toBuilder();

        var defaults = System.getenv().entrySet().stream()
                .filter(e -> e.getKey().startsWith("TEST_CLUSTER_")).map(ClusterFactory::convert)
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (x, y) -> y, LinkedHashMap::new));

        defaults.putIfAbsent(TEST_CLUSTER_IN_VM_DEFAULT, true);
        defaults.putIfAbsent(TEST_CLUSTER_KRAFT_MODE_DEFAULT, true);

        defaults.forEach((key, value) -> {
            switch (key) {
                case TEST_CLUSTER_IN_VM_DEFAULT:
                    if (clusterConfig.getInVM() == null) {
                        builder.inVM(value);
                    }
                    break;
                case TEST_CLUSTER_KRAFT_MODE_DEFAULT:
                    if (clusterConfig.getKraftMode() == null) {
                        builder.kraftMode(value);
                    }
                    break;
                default:
                    LOGGER.warn("Unrecognised environment variable : {}", key);
            }
        });

        var actual = builder.build();
        LOGGER.info("Test cluster : {}", actual);

        if (actual.getInVM()) {
            return new InVMKafkaCluster(actual);
        }
        else {
            return new ContainerBasedKafkaCluster(actual);
        }
    }

    private static Entry<String, Boolean> convert(Entry<String, String> e) {
        return new SimpleEntry<>(e.getKey(), Boolean.parseBoolean(e.getValue()));
    }
}
