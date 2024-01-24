/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.k8s.cmd.KubeCmdClient;

import static java.util.Arrays.asList;

public class HelmClient {
    private static final Logger LOGGER = LogManager.getLogger(HelmClient.class);

    private static final String HELM_CMD = "helm";
    private static final String HELM_3_CMD = "helm3";
    private static final String INSTALL_TIMEOUT_SECONDS = "120s";

    private String namespace;

    private String version;

    private static String helmCommand = HELM_CMD;

    public HelmClient(String namespace) {
        this.namespace = namespace;
    }

    public HelmClient namespace(String namespace) {
        return new HelmClient(namespace);
    }

    /** Install a chart given its local path, release name, and values to override */
    public HelmClient install(String chartName, String releaseName, String version, Map<String, Object> valuesMap) {
        LOGGER.info("Installing helm-chart {}", releaseName);
        String values = Stream.of(valuesMap).flatMap(m -> m.entrySet().stream())
                .map(entry -> String.format("%s=%s", entry.getKey(), entry.getValue()))
                .collect(Collectors.joining(","));
        this.version = version;
        Exec.exec(null, wait(namespace(version(command("install",
                releaseName,
                "--set", values,
                "--timeout", INSTALL_TIMEOUT_SECONDS,
                "--debug",
                chartName)))), 0, true);
        return this;
    }

    public HelmClient install(String chartName, String releaseName, String version) {
        LOGGER.info("Installing helm-chart {}", releaseName);
        this.version = version;
        Exec.exec(null, wait(namespace(version(command("install",
                releaseName,
                "--timeout", INSTALL_TIMEOUT_SECONDS,
                "--debug",
                chartName)))), 0, true);
        return this;
    }

    /** Delete a chart given its release name */
    public HelmClient delete(String releaseName) {
        LOGGER.info("Deleting helm-chart {}", releaseName);
        delete(namespace, releaseName);
        return this;
    }

    /**
     * Delete a Helm chart in specific namespace by given release name
     * @param namespace namespace where chart is installed
     * @param releaseName helm chart release name
     * @return this
     */
    public HelmClient delete(String namespace, String releaseName) {
        LOGGER.info("Deleting helm-chart:{} in namespace:{}", releaseName, namespace);
        Exec.exec(null, wait(command("uninstall", releaseName, "--namespace", namespace)), 0, true);
        return this;
    }

    public static boolean clientAvailable() {
        if (Exec.isExecutableOnPath(HELM_CMD)) {
            return true;
        }
        else if (Exec.isExecutableOnPath(HELM_3_CMD)) {
            helmCommand = HELM_3_CMD;
            return true;
        }
        return false;
    }

    private List<String> command(String... rest) {
        List<String> result = new ArrayList<>();
        result.add(helmCommand);
        result.addAll(asList(rest));
        return result;
    }

    private List<String> version(List<String> args) {
        if (version != null && !version.isEmpty() && !version.equalsIgnoreCase("latest")) {
            args.add("--version");
            args.add(version);
        }
        return args;
    }

    /** Sets namespace for client */
    private List<String> namespace(List<String> args) {
        args.add("--namespace");
        args.add(namespace);
        return args;
    }

    private List<String> wait(List<String> args) {
        args.add("--wait");
        return args;
    }

    static HelmClient findClient(KubeCmdClient<?> kubeClient) {
        HelmClient client = new HelmClient(kubeClient.namespace());
        if (!client.clientAvailable()) {
            throw new RuntimeException("No helm client found on $PATH. $PATH=" + System.getenv("PATH"));
        }
        return client;
    }
}
