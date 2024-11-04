/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collector;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Clusters;

public class ClustersUtil {
    private ClustersUtil() {
    }

    static List<Clusters> distinctClusters(KafkaProxy primary) {
        return distinctClusters(primary.getSpec().getClusters());
    }

    static List<Clusters> distinctClusters(List<Clusters> clusters) {
        return new ArrayList<>(clusters.stream().collect(Collector.of(
                () -> new LinkedHashMap<String, Clusters>(),
                (set, cluster) -> set.putIfAbsent(cluster.getName(), cluster),
                (setA, setB) -> {
                    setA.putAll(setB);
                    return setA;
                })).values());
    }
}
