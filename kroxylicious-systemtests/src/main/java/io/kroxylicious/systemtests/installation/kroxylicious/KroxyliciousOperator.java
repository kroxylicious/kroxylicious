/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.UnsupportedInstallationType;
import io.kroxylicious.systemtests.resources.operator.KroxyliciousOperatorBundleInstaller;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * The type Kroxylicious operator.
 */
public class KroxyliciousOperator {
    private final InstallationMethod installationMethod;
    private final String installationNamespace;

    /**
     * Instantiates a new Kroxylicious operator.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public KroxyliciousOperator(String deploymentNamespace) {
        this.installationNamespace = deploymentNamespace;
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
        return new KroxyliciousOperatorBundleInstaller().getDefaultBuilder(installationNamespace).createBundleInstallation();
    }

    /**
     * Is deployed.
     *
     * @return the boolean
     */
    public boolean isDeployed() {
        Optional<Pod> pod = kubeClient().listPodsByPrefixInName(installationNamespace, Constants.KO_DEPLOYMENT_NAME).stream().findFirst();
        String podName = pod.map(value -> value.getMetadata().getName()).orElse(null);
        return podName != null && kubeClient().isDeploymentRunning(installationNamespace, podName);
    }
}
