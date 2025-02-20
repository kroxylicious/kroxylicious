
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

import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.utils.ImageUtils;
import io.skodjob.testframe.utils.TestFrameUtils;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
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

    private static final Predicate<File> installFiles = file -> !file.getName().contains("Deployment");
    private static final Predicate<File> deploymentFiles = file -> file.getName().contains("Deployment");

    public KroxyliciousOperatorBundleInstaller(String namespaceInstallTo) {
        this.namespaceInstallTo = namespaceInstallTo;
        this.replicas = 1;
        this.extensionContext = ResourceManager.getTestContext();
        this.kroxyliciousOperatorName = Constants.KO_DEPLOYMENT_NAME;
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
        for (File operatorFile : getFilteredOperatorFiles(installFiles)) {
            final String resourceType = operatorFile.getName().split("\\.")[1];

            if (resourceType.equals(Constants.NAMESPACE)) {
                Namespace namespace = TestFrameUtils.configFromYaml(operatorFile, Namespace.class);
                if (!NamespaceUtils.isNamespaceCreated(namespace.getMetadata().getName())) {
                    kubeClient().getClient().resource(namespace).create();
                }
            }
            else {
                try {
                    kubeClient().getClient().load(new FileInputStream(operatorFile.getAbsolutePath()))
                            .inNamespace(namespaceName)
                            .create();
                }
                catch (FileNotFoundException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
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
                .withImage(ImageUtils.changeRegistryOrgAndTag(deploymentImage, Environment.KROXY_REGISTRY, Environment.KROXY_ORG, Environment.KROXY_TAG))
                .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                .endContainer()
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();

        ResourceManager.getInstance().createResourceWithWait(operatorDeployment);
    }

    /**
     * Temporary method to fulfill the Crds installation until new JOSDK 5.0.0 release landed https://github.com/operator-framework/java-operator-sdk/releases
     */
    private void applyCrds() throws FileNotFoundException {
        String path = Constants.PATH_TO_OPERATOR + "/src/main/resources/META-INF/fabric8/";
        File[] files = new File(path).listFiles();
        if (files == null) {
            throw new FileNotFoundException(path + " is empty");
        }
        List<File> crdFiles = Arrays.stream(files).sorted()
                .filter(File::isFile)
                .filter(file -> file.getName().contains("-v1"))
                .toList();
        for (File crdFile : crdFiles) {
            CustomResourceDefinition customResourceDefinition = TestFrameUtils.configFromYaml(crdFile, CustomResourceDefinition.class);
            ResourceManager.getInstance().createResourceWithWait(customResourceDefinition);
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

        prepareEnvForOperator(namespaceInstallTo);
    }

    @Override
    public synchronized void delete() {
        LOGGER.info(SEPARATOR);
        if (IS_EMPTY.test(this)) {
            LOGGER.info("Skip un-installation of the Kroxylicious Operator");
        }
        else {
            LOGGER.info("Un-installing Kroxylicious Operator from Namespace: {}", namespaceInstallTo);

            // clear all resources related to the extension context
            try {
                if (!Environment.SKIP_TEARDOWN) {
                    for (File operatorFile : getFilteredOperatorFiles(installFiles)) {
                        kubeClient().getClient().load(new FileInputStream(operatorFile.getAbsolutePath())).inAnyNamespace().delete();
                    }
                    ResourceManager.getInstance().deleteResources();
                }
            }
            catch (Exception e) {
                Thread.currentThread().interrupt();
                LOGGER.error(e.getStackTrace());
            }
        }
        LOGGER.info(SEPARATOR);
    }
}
