
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.PreconditionViolationException;

import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.LocalObjectReferenceBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.utils.ImageUtils;
import io.skodjob.testframe.utils.PodUtils;
import io.skodjob.testframe.utils.TestFrameUtils;

import io.kroxylicious.kms.service.TestKmsFacadeException;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.resources.manager.ResourceManager;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousSecretTemplates;
import io.kroxylicious.systemtests.utils.CertificateGenerator;
import io.kroxylicious.systemtests.utils.DeploymentUtils;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * KroxyliciousOperatorYamlInstaller encapsulates the whole installation process of Kroxylicious Operator (i.e., RoleBinding, ClusterRoleBinding,
 * ConfigMap, Deployment, CustomResourceDefinition, preparation of the Namespace). Based on the @code{Environment}
 * values, this class installs Kroxylicious Operator using bundle yamls.
 */
public class KroxyliciousOperatorYamlInstaller implements InstallationMethod {

    private static final Logger LOGGER = LogManager.getLogger(KroxyliciousOperatorYamlInstaller.class);
    private static final String SEPARATOR = String.join("", Collections.nCopies(76, "="));

    private static final KubeClusterResource cluster = KubeClusterResource.getInstance();
    protected static final int DEBUG_PORT_NUMBER = 5005;

    private final ExtensionContext extensionContext;
    private final String kroxyliciousOperatorName;
    private final String namespaceInstallTo;
    private Map<String, String> extraLabels;
    private final int replicas;

    private String testClassName;
    // by default, we expect at least empty method name in order to collect logs correctly
    private String testMethodName = "";

    private static final Predicate<KroxyliciousOperatorYamlInstaller> IS_EMPTY = ko -> ko.extensionContext == null && ko.kroxyliciousOperatorName == null
            && ko.namespaceInstallTo == null
            && ko.testClassName == null && ko.testMethodName == null;

    public KroxyliciousOperatorYamlInstaller(String namespaceInstallTo) {
        this.namespaceInstallTo = namespaceInstallTo;
        this.replicas = 1;
        this.extensionContext = KubeResourceManager.get().getTestContext();
        this.kroxyliciousOperatorName = Constants.KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME;
    }

    private KeytoolCertificateGenerator entraCerts(String domain) {
        try {
            KeytoolCertificateGenerator entraCertGen = new CertificateGenerator();
            entraCertGen.generateSelfSignedCertificateEntry("webmaster@example.com", domain,
                    "Engineering", "kroxylicious.io", null, null, "NZ");
             entraCertGen.generateTrustStore(entraCertGen.getCertFilePath(), "website");
            return entraCertGen;
        }
        catch (Exception e) {
            throw new TestKmsFacadeException(e);
        }
    }

    @NonNull
    private static List<Path> installFilesMatching(Predicate<Path> matcher) {
        List<Path> crdFiles;
        try (var fileStream = Files.list(Path.of(Environment.KROXYLICIOUS_OPERATOR_INSTALL_DIR))) {
            crdFiles = fileStream.filter(Files::isRegularFile)
                    .filter(matcher)
                    .sorted()
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return crdFiles;
    }

    @NonNull
    private static Predicate<Path> glob(String glob) {
        PathMatcher pathMatcher = FileSystems.getDefault()
                .getPathMatcher("glob:" + glob);
        return path -> pathMatcher.matches(path.getFileName());
    }

    @NonNull
    private static List<Path> installCrdFiles() {
        return installFilesMatching(glob(Constants.OPERATOR_INSTALL_CRD_GLOB));
    }

    private static File installDeploymentFile() {
        return getFilteredOperatorFiles(glob(Constants.OPERATOR_INSTALL_DEPLOYMENT_GLOB)).get(0);
    }

    private static List<File> installNonDeploymentOrCrdFiles() {
        return getFilteredOperatorFiles(Predicate.not(glob(Constants.OPERATOR_INSTALL_DEPLOYMENT_GLOB)));
    }

    private static List<File> getFilteredOperatorFiles(Predicate<Path> predicate) {
        return installFilesMatching(
                predicate.and(Predicate.not(glob(Constants.OPERATOR_INSTALL_CRD_GLOB)))).stream().map(Path::toFile).toList();
    }

    private void applyClusterOperatorInstallFiles(String namespaceName) {
        DeploymentUtils.deployYamlFiles(namespaceName, installNonDeploymentOrCrdFiles());
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    public void prepareEnvForOperator(String clientNamespace) {
        applyCrds();
        applyClusterOperatorInstallFiles(clientNamespace);
        installCertificates();
        applyDeploymentFile();
    }

    private void installCertificates() {
        KeytoolCertificateGenerator certs = entraCerts(DeploymentUtils.getNodeIP()); //,
                //LowkeyVault.LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME + "." + LowkeyVault.LOWKEY_VAULT_DEFAULT_NAMESPACE + ".svc.cluster.local");
        String defaultNamespace = KubeClusterResource.getInstance().defaultNamespace();
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                KroxyliciousSecretTemplates.createCertificateSecret(Constants.KEYSTORE_SECRET_NAME, defaultNamespace, Constants.KEYSTORE_FILE_NAME,
                        certs.getKeyStoreLocation(),
                        certs.getPassword()));
        ResourceManager.getInstance().createResourceFromBuilderWithWait(
                KroxyliciousSecretTemplates.createCertificateSecret(Constants.TRUSTSTORE_SECRET_NAME, defaultNamespace, Constants.TRUSTSTORE_FILE_NAME,
                        certs.getTrustStoreLocation(), certs.getPassword()));
    }

    private void applyDeploymentFile() {
        Deployment operatorDeployment = TestFrameUtils.configFromYaml(installDeploymentFile(),
                Deployment.class);

        String deploymentImage = operatorDeployment
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .getImage();

        if (Environment.KROXYLICIOUS_OPERATOR_REGISTRY.equals(Environment.KROXYLICIOUS_OPERATOR_REGISTRY_DEFAULT)) {
            deploymentImage = ImageUtils.changeRegistryOrgImageAndTag(deploymentImage, Environment.KROXYLICIOUS_OPERATOR_REGISTRY,
                    Environment.KROXYLICIOUS_OPERATOR_ORG, Environment.KROXYLICIOUS_OPERATOR_IMAGE, Environment.KROXYLICIOUS_OPERATOR_VERSION);
        }

        String debugPortName = "remote-debug";
        //@formatter:off
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
                                .withImage(deploymentImage)
                                .withImagePullPolicy(Constants.PULL_IMAGE_IF_NOT_PRESENT)
                                .addToEnv(new EnvVarBuilder().withName("JAVA_OPTIONS")
                                        .withValue("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:" + DEBUG_PORT_NUMBER)
                                        .build())
                                .addToPorts(new ContainerPortBuilder().withName(debugPortName).withContainerPort(DEBUG_PORT_NUMBER).build())
                            .endContainer()
                            .withImagePullSecrets(new LocalObjectReferenceBuilder()
                                    .withName("regcred")
                                    .build())
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        DeploymentUtils.copySecretInNamespace(namespaceInstallTo, Constants.KEYSTORE_SECRET_NAME);
        DeploymentUtils.copySecretInNamespace(namespaceInstallTo, Constants.TRUSTSTORE_SECRET_NAME);

        if (kubeClient().getClient().secrets().withName(Constants.KEYSTORE_SECRET_NAME).get() != null) {
            operatorDeployment = new DeploymentBuilder(operatorDeployment)
                    .editSpec()
                    .editTemplate()
                    .editSpec()
                    .addToVolumes(new VolumeBuilder()
                            .withName(Constants.KEYSTORE_SECRET_NAME)
                            .withSecret(new SecretVolumeSourceBuilder()
                                    .withSecretName(Constants.KEYSTORE_SECRET_NAME)
                                    .build())
                            .build())
                    .addToVolumes(new VolumeBuilder()
                            .withName(Constants.TRUSTSTORE_SECRET_NAME)
                            .withSecret(new SecretVolumeSourceBuilder()
                                    .withSecretName(Constants.TRUSTSTORE_SECRET_NAME)
                                    .build())
                            .build())
                    .editFirstContainer()
                    .addToVolumeMounts(new VolumeMountBuilder()
                            .withName(Constants.KEYSTORE_SECRET_NAME)
                            .withMountPath(Constants.KEYSTORE_TEMP_DIR)
                            .withReadOnly(true)
                            .build())
                    .addToVolumeMounts(new VolumeMountBuilder()
                            .withName(Constants.TRUSTSTORE_SECRET_NAME)
                            .withMountPath(Constants.TRUSTSTORE_TEMP_DIR)
                            .withReadOnly(true)
                            .build())
                    .endContainer()
                    .endSpec()
                    .endTemplate()
                    .endSpec()
                    .build();
        }

        Service debugService = new ServiceBuilder()
                .editOrNewMetadata()
                    .withName("debug-" + kroxyliciousOperatorName)
                    .withNamespace(namespaceInstallTo)
                .endMetadata()
                .withNewSpec()
                .withSelector(Map.of("app", "kroxylicious"))
                    .withType("LoadBalancer")
                    .withAllocateLoadBalancerNodePorts()
                    .addToPorts(new ServicePortBuilder()
                            .withName(debugPortName)
                            .withPort(DEBUG_PORT_NUMBER)
                            .withTargetPort(new IntOrString(debugPortName))
                            .build())
                .withExternalIPs()
                .endSpec()
                .build();
        //@formatter:on
        KubeResourceManager.get().createOrUpdateResourceWithWait(operatorDeployment, debugService);
        var labels = operatorDeployment.getSpec().getTemplate().getMetadata().getLabels();
        PodUtils.waitForPodsReady(namespaceInstallTo, new LabelSelectorBuilder()
                .withMatchLabels(labels).build(), 1, true, () -> {
                });
    }

    /**
     * Temporary method to fulfill the Crds installation until new JOSDK 5.0.0 release landed https://github.com/operator-framework/java-operator-sdk/releases
     */
    private void applyCrds() {
        for (Path crdPath : installCrdFiles()) {
            CustomResourceDefinition customResourceDefinition = TestFrameUtils.configFromYaml(crdPath.toFile(), CustomResourceDefinition.class);
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
        KroxyliciousOperatorYamlInstaller otherInstallation = (KroxyliciousOperatorYamlInstaller) other;

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
        return "KroxyliciousOperatorYamlInstaller{" +
                "cluster=" + KroxyliciousOperatorYamlInstaller.cluster +
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
                for (File operatorFile : installNonDeploymentOrCrdFiles()) {
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
