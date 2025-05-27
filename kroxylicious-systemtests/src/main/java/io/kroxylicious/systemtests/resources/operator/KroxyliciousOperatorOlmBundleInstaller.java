
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.olm.OperatorSdkRun;
import io.skodjob.testframe.olm.OperatorSdkRunBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.utils.PodUtils;
import io.skodjob.testframe.utils.TestFrameUtils;
import io.skodjob.testframe.wait.Wait;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * KroxyliciousOperatorOlmBundleInstaller encapsulates the whole installation process of Kroxylicious Operator (i.e., RoleBinding, ClusterRoleBinding,
 * ConfigMap, Deployment, CustomResourceDefinition, preparation of the Namespace). Based on the @code{Environment}
 * values, this class installs Kroxylicious Operator using bundle olm.
 */
public class KroxyliciousOperatorOlmBundleInstaller implements InstallationMethod {

    private static final Logger LOGGER = LogManager.getLogger(KroxyliciousOperatorOlmBundleInstaller.class);
    private static final String SEPARATOR = String.join("", Collections.nCopies(76, "="));

    private static final KubeClusterResource cluster = KubeClusterResource.getInstance();

    private static List<File> operatorFiles;

    private final ExtensionContext extensionContext;
    private final String kroxyliciousOperatorName;
    private final String bundleImageRef;
    private final String operatorNamespace;
    private final int replicas;

    private String testClassName;
    // by default, we expect at least empty method name in order to collect logs correctly
    private String testMethodName = "";

    public KroxyliciousOperatorOlmBundleInstaller(String operatorNamespace) {
        this.operatorNamespace = operatorNamespace;
        this.replicas = 1;
        this.extensionContext = KubeResourceManager.get().getTestContext();
        this.kroxyliciousOperatorName = Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME;
        this.bundleImageRef = Environment.KROXYLICIOUS_OPERATOR_BUNDLE_IMAGE;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        KroxyliciousOperatorOlmBundleInstaller otherInstallation = (KroxyliciousOperatorOlmBundleInstaller) other;

        return Objects.equals(kroxyliciousOperatorName, otherInstallation.kroxyliciousOperatorName) &&
                Objects.equals(operatorNamespace, otherInstallation.operatorNamespace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, extensionContext, kroxyliciousOperatorName, operatorNamespace);
    }

    @Override
    public String toString() {
        return "KroxyliciousOperatorOlmBundleInstaller{" +
                "cluster=" + KroxyliciousOperatorOlmBundleInstaller.cluster +
                ", extensionContext=" + extensionContext +
                ", kroxyliciousOperatorName='" + kroxyliciousOperatorName + '\'' +
                ", operatorNamespace='" + operatorNamespace + '\'' +
                ", testClassName='" + testClassName + '\'' +
                ", testMethodName='" + testMethodName + '\'' +
                '}';
    }

    @Override
    public void install() {
        CompletableFuture.completedFuture(install(kroxyliciousOperatorName, operatorNamespace, bundleImageRef));
    }

    private CompletableFuture<Void> install(String operatorName, String operatorNamespace, String bundleImageRef) {
        OperatorSdkRun osr = new OperatorSdkRunBuilder()
                .withBundleImage(bundleImageRef)
                .withInstallMode("AllNamespaces")
                .withNamespace(operatorNamespace)
                .build();

        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
                TestFrameConstants.GLOBAL_TIMEOUT, () -> {
                    TestFrameUtils.runUntilPass(3, () -> {
                        Namespace ns = new NamespaceBuilder()
                                .withNewMetadata()
                                .withName(operatorNamespace)
                                .endMetadata()
                                .build();
                        KubeResourceManager.get().createOrUpdateResourceWithWait(ns);
                        try {
                            return osr.run();
                        } catch (Exception ex) {
                            KubeResourceManager.get().deleteResourceAsyncWait(ns);
                            throw ex;
                        }
                    });
                    return isOperatorReady(operatorNamespace);
                });
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private boolean isOperatorReady(String ns) {
        try {
            PodUtils.waitForPodsReadyWithRestart(ns, new LabelSelectorBuilder()
                            .withMatchLabels(Map.of("app.kubernetes.io/name", "kroxylicious-kubernetes-operator")).build(),
                    1, true);
            LOGGER.info("Kroxylicious operator in namespace {} is ready", ns);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public synchronized void delete() {
        LOGGER.info(SEPARATOR);
        if (Environment.SKIP_TEARDOWN) {
            LOGGER.info("Skip un-installation of the Kroxylicious Operator");
        }
        else {
            LOGGER.info("Un-installing Kroxylicious Operator from Namespace: {}", operatorNamespace);

            // clear all resources related to the extension context
            try {
                KubeResourceManager.get().deleteResources(true);
            }
            catch (Exception e) {
                LOGGER.error("An error occurred when deleting the resources", e);
            }
        }
        LOGGER.info(SEPARATOR);
    }
}
