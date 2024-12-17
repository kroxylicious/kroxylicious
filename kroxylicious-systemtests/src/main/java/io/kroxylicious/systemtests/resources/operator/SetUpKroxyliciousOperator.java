/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;

import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.exception.UnknownInstallationType;

public class SetUpKroxyliciousOperator {
    private final InstallationMethod installationMethod;
    private final String installationNamespace;

    public SetUpKroxyliciousOperator(String installationNamespace) {
        this.installationNamespace = installationNamespace;
        this.installationMethod = getInstallationMethod();
    }

    public void install() {
        installationMethod.install();
    }

    public void delete() {
        installationMethod.delete();
    }

    private InstallationMethod getInstallationMethod() {
        if (Environment.INSTALL_TYPE != InstallType.Yaml) {
            throw new UnknownInstallationType("Installation type " + Environment.INSTALL_TYPE + " not supported");
        }
        return new KroxyliciousOperatorBundleInstaller().getDefaultBuilder(installationNamespace).createBundleInstallation();
    }
}
