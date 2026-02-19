/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;

/**
 * Collects benchmark result files from a Kubernetes pod running
 * OpenMessaging Benchmark.
 */
public class BenchmarkResultCollector {

    private static final String RESULTS_DIR = "/results";

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
     * Finds the benchmark pod, copies result JSON files from {@code /results},
     * and returns the list of local paths written.
     *
     * @param outputDir directory to write result files into
     * @return paths of extracted result JSON files
     * @throws IOException if copying or extraction fails
     */
    public List<Path> collect(Path outputDir) throws IOException {
        Pod pod = findBenchmarkPod();
        String podName = pod.getMetadata().getName();

        try (InputStream tarStream = client.pods()
                .inNamespace(namespace)
                .withName(podName)
                .dir(RESULTS_DIR)
                .read()) {
            return extractTar(tarStream, outputDir);
        }
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

    /**
     * Extracts JSON files from a tar archive stream into the output directory.
     * Non-JSON files and directory entries are skipped.
     *
     * @param tarStream tar archive input stream
     * @param outputDir directory to extract files into
     * @return paths of extracted JSON files
     * @throws IOException if extraction fails
     */
    static List<Path> extractTar(InputStream tarStream, Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        List<Path> extracted = new ArrayList<>();

        try (TarArchiveInputStream tar = new TarArchiveInputStream(tarStream)) {
            TarArchiveEntry entry;
            while ((entry = tar.getNextEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                String fileName = Path.of(entry.getName()).getFileName().toString();
                if (!fileName.endsWith(".json")) {
                    continue;
                }
                Path target = outputDir.resolve(fileName);
                Files.copy(tar, target);
                extracted.add(target);
            }
        }

        return extracted;
    }
}
