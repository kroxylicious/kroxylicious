/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

import static java.util.Arrays.asList;

/**
 * The type Helm client.
 */
public class HelmClient {
    private static final Logger LOGGER = LogManager.getLogger(HelmClient.class);

    private static final String HELM_CMD = "helm";
    private static final String HELM_3_CMD = "helm3";
    private static final String INSTALL_TIMEOUT_SECONDS = "120s";
    private static String helmCommand;
    private Optional<String> namespace;
    private Optional<String> version;

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
        }
        else if (Exec.isExecutableOnPath(HELM_3_CMD)) {
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
        this.namespace = namespace.describeConstable();
        return this;
    }

    /** Install a chart given its name, release name, version and values to override
     * @param chartName the chart name
     * @param releaseName the release name
     * @param version the version
     * @param valuesMap the values to be overridden in the chart, set as key,value
     * @return the helm client
     */
    public HelmClient install(String chartName, String releaseName, String version, Map<String, String> valuesMap) {
        LOGGER.info("Installing helm-chart {}", releaseName);
        String values = valuesMap.entrySet().stream()
                .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
        this.version = Optional.ofNullable(version);
        Exec.exec(null, wait(namespace(version(command("install",
                releaseName,
                "--set", values,
                "--timeout", INSTALL_TIMEOUT_SECONDS,
                "--debug",
                chartName)))), 0, true);
        return this;
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
        Exec.exec(null, command("repo", "add",
                repoName,
                repoUrl), 0, true);
        return this;
    }

    /** Delete a chart given its release name from the currently configured namespace
     * @param releaseName the release name
     * @return the helm client
     */
    public HelmClient delete(String releaseName) {
        LOGGER.info("Deleting helm-chart {}", releaseName);
        delete(String.valueOf(namespace), releaseName);
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
        Exec.exec(null, wait(command("uninstall", releaseName, "--namespace", namespace)), 0, true);
        return this;
    }

    private List<String> command(String... rest) {
        List<String> result = new ArrayList<>();
        result.add(helmCommand);
        result.addAll(asList(rest));
        return result;
    }

    private List<String> version(List<String> args) {
        List<String> result = new ArrayList<>(args);
        version.filter(v -> !v.equals("latest")).ifPresent(v -> result.addAll(List.of("--version", v)));
        return result;
    }

    /** Sets namespace for client */
    private List<String> namespace(List<String> args) {
        List<String> result = new ArrayList<>(args);
        namespace.ifPresent(n -> result.addAll(List.of("--namespace", n)));
        return result;
    }

    private List<String> wait(List<String> args) {
        List<String> result = new ArrayList<>(args);
        result.add("--wait");
        return result;
    }
}
