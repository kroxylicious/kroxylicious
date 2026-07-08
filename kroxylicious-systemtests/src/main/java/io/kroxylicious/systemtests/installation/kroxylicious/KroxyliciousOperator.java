/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.util.Map;

import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;

import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.resources.operator.KroxyliciousOperatorOlmBundleInstaller;
import io.kroxylicious.systemtests.resources.operator.KroxyliciousOperatorYamlInstaller;

/**
 * The type Kroxylicious operator.
 */
public class KroxyliciousOperator {
    private InstallationMethod installationMethod;
    private final String installationNamespace;
    private Map<String, String> operatorEnvVars = Map.of();

    /**
     * Instantiates a new Kroxylicious operator.
     *
     * @param deploymentNamespace the deployment namespace
     */
    public KroxyliciousOperator(String deploymentNamespace) {
        this.installationNamespace = deploymentNamespace;
    }

    /**
     * Sets additional environment variables on the operator pod.
     * Only applies to YAML-based installations.
     *
     * @param envVars environment variables to set
     * @return this
     */
    public KroxyliciousOperator withOperatorEnvVars(Map<String, String> envVars) {
        this.operatorEnvVars = envVars;
        return this;
    }

    /**
     * Deploy.
     */
    public void deploy() {
        this.installationMethod = createInstallationMethod();
        installationMethod.install();
    }

    /**
     * Delete.
     */
    public void delete() {
        if (installationMethod != null) {
            installationMethod.delete();
        }
    }

    private InstallationMethod createInstallationMethod() {
        if (Environment.INSTALL_TYPE == InstallType.Olm) {
            return new KroxyliciousOperatorOlmBundleInstaller(installationNamespace);
        }
        else {
            return new KroxyliciousOperatorYamlInstaller(installationNamespace, operatorEnvVars);
        }
    }
}
