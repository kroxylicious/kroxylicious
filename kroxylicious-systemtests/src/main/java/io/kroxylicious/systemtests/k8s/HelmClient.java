/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static java.util.Arrays.asList;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;

/**
 * The type Helm client.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class HelmClient {
    private static final Logger LOGGER = LogManager.getLogger(HelmClient.class);

    private static final String HELM_CMD = "helm";
    private static final String HELM_3_CMD = "helm3";
    private static final String INSTALL_TIMEOUT_SECONDS = "120s";
    private static String helmCommand;
    private Optional<String> namespace = Optional.empty();

    /**
     * Instantiates a new Helm client.
     */
    public HelmClient() {
        if (!clientAvailable()) {
            throw new KubeClusterException.NotFound("No helm client found on $PATH. $PATH=" + System.getenv("PATH"));
        }
    }

    /**
     * Client available boolean.
     *
     * @return the boolean
     */
    public static boolean clientAvailable() {
        if (Exec.isExecutableOnPath(HELM_CMD)) {
            helmCommand = HELM_CMD;
            return true;
        } else if (Exec.isExecutableOnPath(HELM_3_CMD)) {
            helmCommand = HELM_3_CMD;
            return true;
        }
        return false;
    }

    /**
     * Set the namespace of the helm client.
     *
     * @param namespace the namespace
     * @return the helm client
     */
    public HelmClient namespace(String namespace) {
        this.namespace = Optional.of(namespace);
        return this;
    }

    /**
     * Install a chart given its name, release name, version and values to override
     *
     * @param chartName the chart name
     * @param releaseName the release name
     * @param version the version
     * @param overrideFile helm override file
     * @param overrideMap addition
     * @return the helm client
     */
    public HelmClient install(String chartName, String releaseName, Optional<String> version, Optional<Path> overrideFile, Optional<Map<String, String>> overrideMap) {
        LOGGER.info("Installing helm-chart {}", releaseName);
        var cmd = new ArrayList<String>();
        cmd.add(helmCommand);
        cmd.addAll(List.of("install", releaseName, "--timeout", INSTALL_TIMEOUT_SECONDS, "--debug", chartName));

        version.ifPresent(v -> cmd.addAll(List.of("--version", v)));
        namespace.ifPresent(n -> cmd.addAll(List.of("--namespace", n)));
        overrideFile.map(Path::toString).ifPresent(v -> cmd.addAll(List.of("--values", v)));
        overrideMap.filter(not(Map::isEmpty)).ifPresent(m -> cmd.addAll(List.of("--set", mapToValuesParameterArgument(m))));

        Exec.exec(null, wait(cmd), Duration.ZERO, true);
        return this;
    }

    public HelmClient installByContainerImage(
            String helmRepositoryOci,
            String releaseName,
            Optional<String> version,
            Optional<Path> overrideFile,
            Optional<Map<String, String>> overrideMap
    ) {
        return install("oci://" + helmRepositoryOci, releaseName, version, overrideFile, overrideMap);
    }

    /**
     * Add repository to the helm client.
     *
     * @param repoName the repo name
     * @param repoUrl the repo url
     * @return the helm client
     */
    public HelmClient addRepository(String repoName, String repoUrl) {
        LOGGER.info("Adding repo {}", repoName);
        Exec.exec(
                null,
                command(
                        "repo",
                        "add",
                        repoName,
                        repoUrl
                ),
                Duration.ZERO,
                true
        );
        return this;
    }

    /** Delete a chart given its release name from the currently configured namespace
     * @param releaseName the release name
     * @return the helm client
     */
    public HelmClient delete(String releaseName) {
        LOGGER.info("Deleting helm-chart {}", releaseName);
        delete(namespace.orElse("default"), releaseName);
        return this;
    }

    /**
     * Delete a Helm chart in specific namespace by given release name
     * @param namespace namespace where chart is installed
     * @param releaseName helm chart release name
     * @return this helm client
     */
    public HelmClient delete(String namespace, String releaseName) {
        LOGGER.info("Deleting helm-chart:{} in namespace:{}", releaseName, namespace);
        Exec.exec(null, wait(command("uninstall", releaseName, "--namespace", namespace)), Duration.ZERO, true);
        return this;
    }

    private List<String> command(String... rest) {
        List<String> result = new ArrayList<>();
        result.add(helmCommand);
        result.addAll(asList(rest));
        return result;
    }

    private List<String> wait(List<String> args) {
        List<String> result = new ArrayList<>(args);
        result.add("--wait");
        return result;
    }

    private String mapToValuesParameterArgument(Map<String, String> m) {
        return m.entrySet()
                .stream()
                .map(HelmClient::concatEntry)
                .collect(joining(","));
    }

    private static String concatEntry(Map.Entry<String, String> entry) {
        return String.format("%s=%s", entry.getKey(), entry.getValue());
    }
}
