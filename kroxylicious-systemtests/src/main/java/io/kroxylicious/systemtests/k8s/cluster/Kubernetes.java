/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.cluster;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.k8s.cmd.KubeCmdClient;
import io.kroxylicious.systemtests.k8s.cmd.Kubectl;
import io.kroxylicious.systemtests.k8s.exception.KubeClusterException;

/**
 * A {@link KubeCluster} implementation for any {@code Kubernetes} cluster.
 */
public class Kubernetes implements KubeCluster {

    /**
     * The constant CMD.
     */
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
            return Exec.exec(cmd).isSuccess();
        }
        catch (KubeClusterException e) {
            String commandLine = String.join(" ", cmd);
            LOGGER.error("'{}' failed. Please double check connectivity to your cluster!", commandLine, e);
            return false;
        }
    }

    @Override
    public KubeCmdClient defaultCmdClient() {
        return new Kubectl();
    }

    public String toString() {
        return CMD;
    }
}
