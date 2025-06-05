
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
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
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * KroxyliciousOperatorOlmBundleInstaller encapsulates the whole OLM installation process of Kroxylicious Operator. Based on the @code{Environment}
 * values, this class installs Kroxylicious Operator using bundle olm.
 */
public class KroxyliciousOperatorOlmBundleInstaller implements InstallationMethod {

    private static final Logger LOGGER = LogManager.getLogger(KroxyliciousOperatorOlmBundleInstaller.class);
    private static final String SEPARATOR = String.join("", Collections.nCopies(76, "="));

    private final ExtensionContext extensionContext;
    private final String kroxyliciousOperatorName;
    private final String bundleImageRef;
    private final String operatorNamespace;

    public KroxyliciousOperatorOlmBundleInstaller(String operatorNamespace) {
        this.operatorNamespace = operatorNamespace;
        this.extensionContext = KubeResourceManager.get().getTestContext();
        this.kroxyliciousOperatorName = Environment.KROXYLICIOUS_OLM_DEPLOYMENT_NAME;
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
                Objects.equals(operatorNamespace, otherInstallation.operatorNamespace) &&
                Objects.equals(bundleImageRef, otherInstallation.bundleImageRef) &&
                Objects.equals(extensionContext, otherInstallation.extensionContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(extensionContext, kroxyliciousOperatorName, operatorNamespace, bundleImageRef);
    }

    @Override
    public String toString() {
        return "KroxyliciousOperatorOlmBundleInstaller{" +
                ", extensionContext=" + extensionContext +
                ", kroxyliciousOperatorName='" + kroxyliciousOperatorName + '\'' +
                ", operatorNamespace='" + operatorNamespace + '\'' +
                ", bundleImageRef='" + bundleImageRef + '\'' +
                '}';
    }

    @Override
    public void install() {
        LOGGER.info("Setup Kroxylicious Operator using OLM");
        if (bundleImageRef != null && !bundleImageRef.isEmpty()) {
            CompletableFuture.completedFuture(install(kroxyliciousOperatorName, operatorNamespace, bundleImageRef));
        }
        else {
            CompletableFuture.completedFuture(install(kroxyliciousOperatorName, operatorNamespace,
                    Environment.OLM_OPERATOR_CHANNEL, Environment.CATALOG_SOURCE_NAME, "openshift-marketplace"));
        }
    }

    /**
     * Install strimzi operator from catalog presented on cluster using OLM
     *
     * @param operatorName      name of operator
     * @param operatorNamespace where operator will be present
     * @param channel           chanel
     * @param source            source name of catalog
     * @param catalogNs         source catalog namespace
     * @return wait future
     */
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    public CompletableFuture<Void> install(String operatorName, String operatorNamespace,
                                           String channel, String source, String catalogNs) {
        // Create ns for the operator
        NamespaceUtils.createNamespaceAndPrepare(operatorNamespace);
        // Create operator group for the operator
        if (KubeResourceManager.get().kubeClient().getOpenShiftClient().operatorHub().operatorGroups()
                .inNamespace(operatorNamespace).list().getItems().isEmpty()) {
            OperatorGroupBuilder operatorGroup = new OperatorGroupBuilder()
                    .editOrNewMetadata()
                    .withName("amq-streams-proxy-operator-group")
                    .withNamespace(operatorNamespace)
                    .endMetadata();
            ResourceManager.getInstance().createResourceFromBuilderWithWait(operatorGroup);
        }
        else {
            LOGGER.info("OperatorGroup already exists.");
        }

        // @formatter:off
        Subscription subscription = new SubscriptionBuilder()
                .editOrNewMetadata()
                    .withName(Constants.KROXYLICIOUS_OPERATOR_SUBSCRIPTION_NAME)
                    .withNamespace(operatorNamespace)
                .endMetadata()
                .editOrNewSpec()
                    .withName(operatorName)
                    .withChannel(channel)
                    .withSource(source)
                    .withSourceNamespace(catalogNs)
                    .withInstallPlanApproval("Automatic")
                .endSpec()
                .build();
        // @formatter:on

        KubeResourceManager.get().createOrUpdateResourceWithoutWait(subscription);
        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
                TestFrameConstants.GLOBAL_TIMEOUT, () -> isOperatorReady(operatorNamespace));
    }

    private CompletableFuture<Void> install(String operatorName, String operatorNamespace, String bundleImageRef) {
        OperatorSdkRun osr = new OperatorSdkRunBuilder()
                .withBundleImage(bundleImageRef)
                .withInstallMode("AllNamespaces")
                .withNamespace(operatorNamespace)
                .build();

        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
                TestFrameConstants.GLOBAL_TIMEOUT, () -> {
                    NamespaceUtils.createNamespaceAndPrepare(operatorNamespace);
                    TestFrameUtils.runUntilPass(3, osr::run);
                    return isOperatorReady(operatorNamespace);
                });
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private boolean isOperatorReady(String ns) {
        try {
            PodUtils.waitForPodsReadyWithRestart(ns, new LabelSelectorBuilder()
                    .withMatchLabels(Map.of("app.kubernetes.io/instance", Constants.KROXYLICIOUS_OPERATOR_OLM_LABEL)).build(),
                    1, true);
            LOGGER.info("Kroxylicious operator in namespace {} is ready", ns);
            return true;
        }
        catch (Exception ex) {
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
