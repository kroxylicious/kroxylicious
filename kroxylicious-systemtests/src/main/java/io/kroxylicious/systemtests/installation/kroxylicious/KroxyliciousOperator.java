/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.UnsupportedInstallationType;
import io.kroxylicious.systemtests.resources.operator.KroxyliciousOperatorBundleInstaller;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;
import static org.awaitility.Awaitility.await;

/**
 * The type Kroxylicious operator.
 */
public class KroxyliciousOperator {
    private static final Logger LOGGER = LogManager.getLogger(KroxyliciousOperator.class);
    private final InstallationMethod installationMethod;
    private final String installationNamespace;
    private final int replicas;

    /**
     * Instantiates a new Kroxylicious operator.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public KroxyliciousOperator(String deploymentNamespace, int replicas) {
        this.installationNamespace = deploymentNamespace;
        this.replicas = replicas;
        this.installationMethod = getInstallationMethod();
    }

    /**
     * Deploy.
     */
    public void deploy() {
        installationMethod.install();
    }

    /**
     * Delete.
     */
    public void delete() {
        installationMethod.delete();
    }

    private InstallationMethod getInstallationMethod() {
        if (Environment.INSTALL_TYPE != InstallType.Yaml) {
            throw new UnsupportedInstallationType("Installation type " + Environment.INSTALL_TYPE + " not supported");
        }
        return new KroxyliciousOperatorBundleInstaller(installationNamespace, replicas);
    }

    /**
     * Gets number of replicas.
     *
     * @return the number of replicas
     */
    public int getNumberOfReplicas() {
        LOGGER.info("Getting number of replicas..");
        return kubeClient().getDeployment(installationNamespace, Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME).getStatus().getReplicas();
    }

    /**
     * Scale replicas to.
     *
     * @param scaledTo the number of replicas to scale up/down
     * @param timeout the timeout
     */
    public void scaleReplicasTo(int scaledTo, Duration timeout) {
        LOGGER.info("Scaling number of replicas to {}..", scaledTo);
        kubeClient().getClient().apps().deployments().inNamespace(installationNamespace).withName(Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME).scale(scaledTo);
        await().atMost(timeout).pollInterval(Duration.ofSeconds(1))
                .until(() -> getNumberOfReplicas() == scaledTo && kubeClient().isDeploymentReady(installationNamespace, Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME));
    }
}
