/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.util.List;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Collects benchmark result files from a Kubernetes pod running
 * OpenMessaging Benchmark.
 */
public class BenchmarkResultCollector {

    private final KubernetesClient client;
    private final String namespace;
    private final String labelKey;
    private final String labelValue;

    /**
     * Creates a new collector.
     *
     * @param client Kubernetes client
     * @param namespace namespace to search for the benchmark pod
     * @param podLabel label selector in {@code key=value} format
     */
    public BenchmarkResultCollector(KubernetesClient client, String namespace, String podLabel) {
        this.client = client;
        this.namespace = namespace;
        String[] parts = podLabel.split("=", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("podLabel must be in key=value format: " + podLabel);
        }
        this.labelKey = parts[0];
        this.labelValue = parts[1];
    }

    /**
     * Finds the benchmark pod by label in the configured namespace.
     *
     * @return the first matching pod
     * @throws IllegalStateException if no matching pod is found
     */
    public Pod findBenchmarkPod() {
        List<Pod> pods = client.pods()
                .inNamespace(namespace)
                .withLabel(labelKey, labelValue)
                .list()
                .getItems();

        if (pods.isEmpty()) {
            throw new IllegalStateException(
                    "no benchmark pod found with label %s=%s in namespace %s".formatted(labelKey, labelValue, namespace));
        }

        return pods.get(0);
    }
}
