
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.PreconditionViolationException;

import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.utils.ImageUtils;
import io.skodjob.testframe.utils.TestFrameUtils;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * KroxyliciousOperatorBundleInstaller encapsulates the whole installation process of Kroxylicious Operator (i.e., RoleBinding, ClusterRoleBinding,
 * ConfigMap, Deployment, CustomResourceDefinition, preparation of the Namespace). Based on the @code{Environment}
 * values, this class installs Kroxylicious Operator using bundle yamls.
 */
public class KroxyliciousOperatorBundleInstaller implements InstallationMethod {

    private static final Logger LOGGER = LogManager.getLogger(KroxyliciousOperatorBundleInstaller.class);
    private static final String SEPARATOR = String.join("", Collections.nCopies(76, "="));

    private static final KubeClusterResource cluster = KubeClusterResource.getInstance();

    private static List<File> operatorFiles;

    private final ExtensionContext extensionContext;
    private final String kroxyliciousOperatorName;
    private final String namespaceInstallTo;
    private Map<String, String> extraLabels;
    private final int replicas;

    private String testClassName;
    // by default, we expect at least empty method name in order to collect logs correctly
    private String testMethodName = "";

    private static final Predicate<KroxyliciousOperatorBundleInstaller> IS_EMPTY = ko -> ko.extensionContext == null && ko.kroxyliciousOperatorName == null
            && ko.namespaceInstallTo == null
            && ko.testClassName == null && ko.testMethodName == null;

    private static final Predicate<File> deploymentFiles = file -> file.getName().contains("Deployment");

    public KroxyliciousOperatorBundleInstaller(String namespaceInstallTo) {
        this.namespaceInstallTo = namespaceInstallTo;
        this.replicas = 1;
        this.extensionContext = KubeResourceManager.get().getTestContext();
        this.kroxyliciousOperatorName = Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME;
    }

    private static List<File> getOperatorFiles() {
        if (operatorFiles == null || operatorFiles.isEmpty()) {
            operatorFiles = Arrays.stream(Objects.requireNonNull(new File(Constants.PATH_TO_OPERATOR_INSTALL_FILES).listFiles())).sorted()
                    .filter(File::isFile)
                    .toList();
        }
        return operatorFiles;
    }

    private static List<File> getFilteredOperatorFiles(Predicate<File> predicate) {
        return getOperatorFiles().stream().filter(predicate).toList();
    }

    private void applyClusterOperatorInstallFiles(String namespaceName) {
        DeploymentUtils.deployYamlFiles(namespaceName, getFilteredOperatorFiles(Predicate.not(deploymentFiles)));
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    public void prepareEnvForOperator(String clientNamespace) {
        try {
            applyCrds();
        }
        catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
        applyClusterOperatorInstallFiles(clientNamespace);
        applyDeploymentFile();
    }

    private void applyDeploymentFile() {
        Deployment operatorDeployment = TestFrameUtils.configFromYaml(getFilteredOperatorFiles(deploymentFiles).get(0), Deployment.class);

        String deploymentImage = operatorDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getImage();

        operatorDeployment = new DeploymentBuilder(operatorDeployment)
                .editOrNewMetadata()
                .withName(kroxyliciousOperatorName)
                .withNamespace(namespaceInstallTo)
                .addToLabels(Constants.DEPLOYMENT_TYPE, InstallType.Yaml.name())
                .endMetadata()
                .editSpec()
                .withReplicas(this.replicas)
                .editOrNewSelector()
                .addToMatchLabels(this.extraLabels)
                .endSelector()
                .editTemplate()
                .editMetadata()
                .addToLabels(this.extraLabels)
                .endMetadata()
                .editSpec()
                .editFirstContainer()
                .withImage(ImageUtils.changeRegistryOrgImageAndTag(deploymentImage, Environment.KROXYLICIOUS_OPERATOR_REGISTRY,
                        Environment.KROXYLICIOUS_OPERATOR_ORG, Environment.KROXYLICIOUS_OPERATOR_IMAGE, Environment.KROXYLICIOUS_OPERATOR_VERSION))
                .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                .endContainer()
                .withImagePullSecrets(new LocalObjectReferenceBuilder()
                        .withName("regcred")
                        .build())
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        KubeResourceManager.get().createOrUpdateResourceWithWait(operatorDeployment);
    }

    /**
     * Temporary method to fulfill the Crds installation until new JOSDK 5.0.0 release landed https://github.com/operator-framework/java-operator-sdk/releases
     */
    private void applyCrds() throws FileNotFoundException {
        File[] files = new File(Constants.PATH_TO_CRDS).listFiles();
        if (files == null) {
            throw new FileNotFoundException(Constants.PATH_TO_CRDS + " is empty");
        }
        List<File> crdFiles = Arrays.stream(files).sorted()
                .filter(File::isFile)
                .filter(file -> file.getName().contains("-v1"))
                .toList();
        for (File crdFile : crdFiles) {
            CustomResourceDefinition customResourceDefinition = TestFrameUtils.configFromYaml(crdFile, CustomResourceDefinition.class);
            KubeResourceManager.get().createOrUpdateResourceWithWait(customResourceDefinition);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        KroxyliciousOperatorBundleInstaller otherInstallation = (KroxyliciousOperatorBundleInstaller) other;

        return Objects.equals(kroxyliciousOperatorName, otherInstallation.kroxyliciousOperatorName) &&
                Objects.equals(namespaceInstallTo, otherInstallation.namespaceInstallTo) &&
                Objects.equals(extraLabels, otherInstallation.extraLabels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, extensionContext, kroxyliciousOperatorName, namespaceInstallTo, extraLabels);
    }

    @Override
    public String toString() {
        return "KroxyliciousOperatorBundleInstaller{" +
                "cluster=" + KroxyliciousOperatorBundleInstaller.cluster +
                ", extensionContext=" + extensionContext +
                ", kroxyliciousOperatorName='" + kroxyliciousOperatorName + '\'' +
                ", namespaceInstallTo='" + namespaceInstallTo + '\'' +
                ", extraLabels=" + extraLabels +
                ", testClassName='" + testClassName + '\'' +
                ", testMethodName='" + testMethodName + '\'' +
                '}';
    }

    private void setTestClassNameAndTestMethodName() {
        this.testClassName = this.extensionContext.getRequiredTestClass() != null ? this.extensionContext.getRequiredTestClass().getName() : "";

        try {
            if (this.extensionContext.getRequiredTestMethod() != null) {
                this.testMethodName = this.extensionContext.getRequiredTestMethod().getName();
            }
        }
        catch (PreconditionViolationException e) {
            LOGGER.debug("Test method is not present: {}\n{}", e.getMessage(), e.getCause());
            // getRequiredTestMethod() is not present, in @BeforeAll scope so we're avoiding PreconditionViolationException exception
            this.testMethodName = "";
        }
    }

    @Override
    public void install() {
        LOGGER.info("Install Kroxylicious Operator via Yaml bundle in namespace {}", namespaceInstallTo);

        setTestClassNameAndTestMethodName();
        NamespaceUtils.addNamespaceToSet(Constants.KROXYLICIOUS_OPERATOR_NAMESPACE, testClassName);
        prepareEnvForOperator(namespaceInstallTo);
    }

    @Override
    public synchronized void delete() {
        LOGGER.info(SEPARATOR);
        if (IS_EMPTY.test(this) || Environment.SKIP_TEARDOWN) {
            LOGGER.info("Skip un-installation of the Kroxylicious Operator");
        }
        else {
            LOGGER.info("Un-installing Kroxylicious Operator from Namespace: {}", namespaceInstallTo);

            // clear all resources related to the extension context
            try {
                for (File operatorFile : getFilteredOperatorFiles(Predicate.not(deploymentFiles))) {
                    LOGGER.info("Deleting Kroxylicious Operator element: {}", operatorFile.getName());
                    kubeClient().getClient().load(new FileInputStream(operatorFile.getAbsolutePath())).inAnyNamespace().delete();
                }
                KubeResourceManager.get().deleteResources(true);
            }
            catch (Exception e) {
                LOGGER.error("An error occurred when deleting the resources", e);
            }
        }
        LOGGER.info(SEPARATOR);
    }
}
