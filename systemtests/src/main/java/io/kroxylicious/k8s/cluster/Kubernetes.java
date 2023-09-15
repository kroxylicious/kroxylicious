/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.cluster;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import io.kroxylicious.executor.Exec;
import io.kroxylicious.k8s.cmd.KubeCmdClient;
import io.kroxylicious.k8s.cmd.Kubectl;
import io.kroxylicious.k8s.exception.KubeClient;
import io.kroxylicious.k8s.exception.KubeClusterException;

/**
 * A {@link KubeCluster} implementation for any {@code Kubernetes} cluster.
 */
public class Kubernetes implements KubeCluster {

    public static final String CMD = "kubectl";
    private static final Logger LOGGER = LoggerFactory.getLogger(Kubernetes.class);

    @Override
    public boolean isAvailable() {
        return Exec.isExecutableOnPath(CMD);
    }

    @Override
    public boolean isClusterUp() {
        List<String> cmd = Arrays.asList(CMD, "cluster-info");
        try {
            return Exec.exec(cmd).exitStatus();
        }
        catch (KubeClusterException e) {
            LOGGER.debug("'" + String.join(" ", cmd) + "' failed. Please double check connectivity to your cluster!");
            LOGGER.debug(String.valueOf(e));
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Kubectl();
    }

    @Override
    public KubeClient defaultClient() {
        return new KubeClient(new KubernetesClientBuilder().build(), "default");
    }

    public String toString() {
        return CMD;
    }
}
