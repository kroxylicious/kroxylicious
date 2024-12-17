
/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources.operator;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.PreconditionViolationException;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.skodjob.testframe.installation.InstallationMethod;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.Environment;
import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.k8s.KubeClusterResource;
import io.kroxylicious.systemtests.k8s.exception.UnknownInstallationType;
import io.kroxylicious.systemtests.resources.ResourceItem;
import io.kroxylicious.systemtests.resources.ResourceManager;
import io.kroxylicious.systemtests.resources.kubernetes.ClusterRoleBindingResource;
import io.kroxylicious.systemtests.templates.kubernetes.ClusterRoleBindingTemplates;
import io.kroxylicious.systemtests.utils.NamespaceUtils;
import io.kroxylicious.systemtests.utils.ReadWriteUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static io.kroxylicious.systemtests.k8s.KubeClusterResource.kubeClient;

/**
 * KroxyliciousOperatorBundleInstaller encapsulates the whole installation process of Kroxylicious Operator (i.e., RoleBinding, ClusterRoleBinding,
 * ConfigMap, Deployment, CustomResourceDefinition, preparation of the Namespace). Based on the @code{Environment}
 * values, this class installs Kroxylicious Operator using bundle yamls.
 */
public class KroxyliciousOperatorBundleInstaller implements InstallationMethod {

    private static final Logger LOGGER = LogManager.getLogger(KroxyliciousOperatorBundleInstaller.class);
    private static final String SEPARATOR = String.join("", Collections.nCopies(76, "="));

    private static KubeClusterResource cluster = KubeClusterResource.getInstance();

    private final ExtensionContext extensionContext;
    private String kroxyliciousOperatorName;
    private String namespaceInstallTo;
    private String namespaceToWatch;
    private List<String> bindingsNamespaces;
    private Duration operationTimeout;
    private Duration reconciliationInterval;
    private Map<String, String> extraLabels;
    private final int replicas;

    private String testClassName;
    // by default, we expect at least empty method name in order to collect logs correctly
    private String testMethodName = "";

    private static final Predicate<KroxyliciousOperatorBundleInstaller> IS_EMPTY = ko -> ko.extensionContext == null && ko.kroxyliciousOperatorName == null
            && ko.namespaceInstallTo == null &&
            ko.namespaceToWatch == null && ko.bindingsNamespaces == null && ko.operationTimeout == null && ko.reconciliationInterval == null
            && ko.testClassName == null && ko.testMethodName == null;

    public KroxyliciousOperatorBundleInstaller() {
        this.namespaceInstallTo = Constants.KO_NAMESPACE;
        this.replicas = 1;
        this.extensionContext = ResourceManager.getTestContext();
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public KroxyliciousOperatorBundleInstaller(KroxyliciousOperatorBuilder builder) {
        this.extensionContext = builder.extensionContext;
        this.kroxyliciousOperatorName = builder.kroxyliciousOperatorName;
        this.namespaceInstallTo = builder.namespaceInstallTo;
        this.namespaceToWatch = builder.namespaceToWatch;
        this.bindingsNamespaces = builder.bindingsNamespaces;
        this.operationTimeout = builder.operationTimeout;
        this.reconciliationInterval = builder.reconciliationInterval;
        this.extraLabels = builder.extraLabels;
        this.replicas = builder.replicas;

        // assign defaults is something is not specified
        if (this.kroxyliciousOperatorName == null || this.kroxyliciousOperatorName.isEmpty()) {
            this.kroxyliciousOperatorName = Constants.KO_DEPLOYMENT_NAME;
        }
        // if namespace is not set we install operator to 'kroxylicious-operator'
        if (this.namespaceInstallTo == null || this.namespaceInstallTo.isEmpty()) {
            this.namespaceInstallTo = Constants.KO_NAMESPACE;
        }
        if (this.namespaceToWatch == null) {
            this.namespaceToWatch = this.namespaceInstallTo;
        }
        if (this.bindingsNamespaces == null) {
            this.bindingsNamespaces = new ArrayList<>();
            this.bindingsNamespaces.add(this.namespaceInstallTo);
        }
        if (this.operationTimeout == null) {
            this.operationTimeout = Constants.KO_OPERATION_TIMEOUT_DEFAULT;
        }
        if (this.reconciliationInterval == null) {
            this.reconciliationInterval = Constants.RECONCILIATION_INTERVAL;
        }
        if (this.extraLabels == null) {
            this.extraLabels = new HashMap<>();
        }
    }

    private boolean isKroxyliciousOperatorNamespaceNotCreated() {
        return extensionContext == null
                || extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo) == null;
    }

    private void createKroxyliciousOperatorNamespaceIfPossible() {
        if (isKroxyliciousOperatorNamespaceNotCreated()) {
            cluster.setNamespace(namespaceInstallTo);

            if (this.extensionContext != null) {
                ResourceManager.STORED_RESOURCES.computeIfAbsent(this.extensionContext.getDisplayName(), k -> new Stack<>());
                ResourceManager.STORED_RESOURCES.get(this.extensionContext.getDisplayName()).push(
                        new ResourceItem<>(this::deleteKroxyliciousOperatorNamespace));

                NamespaceUtils.createNamespaces(namespaceInstallTo, bindingsNamespaces);

                this.extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, true);
            }
        }
    }

    /**
     * Perform application of ServiceAccount, Roles and CRDs needed for proper cluster operator deployment.
     * Configuration files are loaded from kroxylicious-operator directory.
     */
    public void applyClusterOperatorInstallFiles(String namespaceName) {
        List<File> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(Constants.PATH_TO_OPERATOR_INSTALL_FILES).listFiles())).sorted()
                .filter(File::isFile)
                .filter(file -> !file.getName().contains("Binding") && !file.getName().contains("Deployment"))
                .toList();

        for (File operatorFile : operatorFiles) {
            final String resourceType = operatorFile.getName().split("\\.")[1];

            switch (resourceType) {
                case Constants.NAMESPACE:
                    Namespace namespace = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, Namespace.class);
                    ResourceManager.getInstance().createResourceWithWait(new NamespaceBuilder(namespace)
                            .editMetadata()
                            .endMetadata()
                            .build());
                    break;
                case Constants.ROLE:
                    Role role = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, Role.class);
                    ResourceManager.getInstance().createResourceWithWait(new RoleBuilder(role)
                            .editMetadata()
                            .withNamespace(namespaceName)
                            .endMetadata()
                            .build());
                    break;
                case Constants.CLUSTER_ROLE:
                    ClusterRole clusterRole = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile.getAbsolutePath(), ClusterRole.class);
                    ResourceManager.getInstance().createResourceWithWait(clusterRole);
                    break;
                case Constants.SERVICE_ACCOUNT:
                    ServiceAccount serviceAccount = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, ServiceAccount.class);
                    ResourceManager.getInstance().createResourceWithWait(new ServiceAccountBuilder(serviceAccount)
                            .editMetadata()
                            .withNamespace(namespaceName)
                            .endMetadata()
                            .build());
                    break;
                case Constants.CONFIG_MAP:
                    ConfigMap configMap = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, ConfigMap.class);
                    ResourceManager.getInstance().createResourceWithWait(new ConfigMapBuilder(configMap)
                            .editMetadata()
                            .withNamespace(namespaceName)
                            .withName(kroxyliciousOperatorName)
                            .endMetadata()
                            .build());
                    break;
                case Constants.CUSTOM_RESOURCE_DEFINITION_SHORT:
                    CustomResourceDefinition customResourceDefinition = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, CustomResourceDefinition.class);
                    ResourceManager.getInstance().createResourceWithWait(customResourceDefinition);
                    break;
                default:
                    LOGGER.error("Unknown installation resource type: {}", resourceType);
                    throw new UnknownInstallationType("Unknown installation resource type:" + resourceType);
            }
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     */
    public void prepareEnvForOperator(String clientNamespace, List<String> namespaces) {
        applyCrds();
        applyClusterOperatorInstallFiles(clientNamespace);

        if (cluster.cluster().isOpenshift() && kubeClient().getNamespace(Environment.KROXY_ORG) != null) {
            for (String namespace : namespaces) {
                LOGGER.debug("Setting group policy for Openshift registry in Namespace: {}", namespace);
                Exec.exec(Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.KROXY_ORG));
            }
        }
    }

    /**
     * Temporary method to fulfill the Crds installation until new JOSDK 5.0.0 release landed https://github.com/operator-framework/java-operator-sdk/releases
     */
    private void applyCrds() {
        String path = Constants.PATH_TO_OPERATOR + "/src/main/resources/META-INF/fabric8/";
        List<File> operatorFiles = Arrays.stream(Objects.requireNonNull(new File(path).listFiles())).sorted()
                .filter(File::isFile)
                .filter(file -> file.getName().contains("-v1"))
                .toList();
        for (File operatorFile : operatorFiles) {
            CustomResourceDefinition customResourceDefinition = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, CustomResourceDefinition.class);
            ResourceManager.getInstance().createResourceWithWait(customResourceDefinition);
        }
    }

    private void deleteKroxyliciousOperatorNamespace() {
        LOGGER.info("Deleting Namespace {}", this.namespaceInstallTo);
        NamespaceUtils.deleteNamespaceWithWait(this.namespaceInstallTo);
    }

    private void createClusterRoleBindings() {
        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(namespaceInstallTo);
        clusterRoleBindingList.forEach(clusterRoleBinding -> ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding));
    }

    /**
     * Method to apply Kroxylicious operator specific ClusterRoleBindings for specific namespaces.
     */
    public void applyDefaultBindings() {
        // cluster-wide installation
        if (namespaceToWatch.equals(Constants.WATCH_ALL_NAMESPACES)) {
            createClusterRoleBindings();
        }
        else {
            applyClusterRoleBindings(this.namespaceInstallTo);
        }
    }

    private static void applyClusterRoleBindings(String namespace) {
        ClusterRoleBindingResource.clusterRoleBinding(namespace,
                Constants.PATH_TO_OPERATOR_INSTALL_FILES + "/02.ClusterRoleBinding.kroxylicious-operator-dependent.yaml");
        ClusterRoleBindingResource.clusterRoleBinding(namespace,
                Constants.PATH_TO_OPERATOR_INSTALL_FILES + "/02.ClusterRoleBinding.kroxylicious-operator-filter-record-encryption.yaml");
        ClusterRoleBindingResource.clusterRoleBinding(namespace,
                Constants.PATH_TO_OPERATOR_INSTALL_FILES + "/02.ClusterRoleBinding.kroxylicious-operator-filter-record-validation.yaml");
        ClusterRoleBindingResource.clusterRoleBinding(namespace,
                Constants.PATH_TO_OPERATOR_INSTALL_FILES + "/02.ClusterRoleBinding.kroxylicious-operator-watched.yaml");
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

        return operationTimeout == otherInstallation.operationTimeout &&
                reconciliationInterval == otherInstallation.reconciliationInterval &&
                Objects.equals(cluster, KroxyliciousOperatorBundleInstaller.cluster) &&
                Objects.equals(kroxyliciousOperatorName, otherInstallation.kroxyliciousOperatorName) &&
                Objects.equals(namespaceInstallTo, otherInstallation.namespaceInstallTo) &&
                Objects.equals(namespaceToWatch, otherInstallation.namespaceToWatch) &&
                Objects.equals(bindingsNamespaces, otherInstallation.bindingsNamespaces) &&
                Objects.equals(extraLabels, otherInstallation.extraLabels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cluster, extensionContext,
                kroxyliciousOperatorName, namespaceInstallTo, namespaceToWatch, bindingsNamespaces, operationTimeout, extraLabels);
    }

    @Override
    public String toString() {
        return "KroxyliciousOperatorBundleInstaller{" +
                "cluster=" + KroxyliciousOperatorBundleInstaller.cluster +
                ", extensionContext=" + extensionContext +
                ", kroxyliciousOperatorName='" + kroxyliciousOperatorName + '\'' +
                ", namespaceInstallTo='" + namespaceInstallTo + '\'' +
                ", namespaceToWatch='" + namespaceToWatch + '\'' +
                ", bindingsNamespaces=" + bindingsNamespaces +
                ", operationTimeout=" + operationTimeout +
                ", reconciliationInterval=" + reconciliationInterval +
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

        // check if namespace is already created
        createKroxyliciousOperatorNamespaceIfPossible();
        prepareEnvForOperator(namespaceInstallTo, bindingsNamespaces);
        LOGGER.info("Install default bindings");
        applyDefaultBindings();

        // 03.Deployment
        ResourceManager.setKoDeploymentName(kroxyliciousOperatorName);
        ResourceManager.getInstance().createResourceWithWait(
                new BundleResource.BundleResourceBuilder()
                        .withReplicas(replicas)
                        .withName(kroxyliciousOperatorName)
                        .withNamespace(namespaceInstallTo)
                        .withWatchingNamespaces(namespaceToWatch)
                        .withOperationTimeout(operationTimeout)
                        .withReconciliationInterval(reconciliationInterval)
                        .withExtraLabels(extraLabels)
                        .buildBundleInstance()
                        .buildBundleDeployment()
                        .build());
    }

    @Override
    public synchronized void delete() {
        LOGGER.info(SEPARATOR);
        if (IS_EMPTY.test(this)) {
            LOGGER.info("Skip un-installation of the Kroxylicious Operator");
        }
        else {
            LOGGER.info("Un-installing Kroxylicious Operator from Namespace: {}", namespaceInstallTo);

            if (this.extensionContext != null) {
                this.extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, null);
            }

            // clear all resources related to the extension context
            try {
                if (!Environment.SKIP_TEARDOWN) {
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

    public KroxyliciousOperatorBuilder getDefaultBuilder(String installationNamespace) {
        return new KroxyliciousOperatorBuilder()
                .withExtensionContext(ResourceManager.getTestContext())
                .withNamespace(installationNamespace)
                .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES);
    }

    public static class KroxyliciousOperatorBuilder {

        private ExtensionContext extensionContext;
        private String kroxyliciousOperatorName;
        private String namespaceInstallTo;
        private String namespaceToWatch;
        private List<String> bindingsNamespaces;
        private Duration operationTimeout;
        private Duration reconciliationInterval;
        private Map<String, String> extraLabels;
        private int replicas = 1;

        public KroxyliciousOperatorBuilder withExtensionContext(ExtensionContext extensionContext) {
            this.extensionContext = extensionContext;
            return self();
        }

        public KroxyliciousOperatorBuilder withKroxyliciousOperatorName(String kroxyliciousOperatorName) {
            this.kroxyliciousOperatorName = kroxyliciousOperatorName;
            return self();
        }

        public KroxyliciousOperatorBuilder withNamespace(String namespaceInstallTo) {
            this.namespaceInstallTo = namespaceInstallTo;
            return self();
        }

        public KroxyliciousOperatorBuilder withWatchingNamespaces(String namespaceToWatch) {
            this.namespaceToWatch = namespaceToWatch;
            return self();
        }

        public KroxyliciousOperatorBuilder addToTheWatchingNamespaces(String namespaceToWatch) {
            if (this.namespaceToWatch != null) {
                if (!this.namespaceToWatch.equals("*")) {
                    this.namespaceToWatch += "," + namespaceToWatch;
                }
            }
            else {
                this.namespaceToWatch = namespaceToWatch;
            }
            return self();
        }

        public KroxyliciousOperatorBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
            this.bindingsNamespaces = bindingsNamespaces;
            return self();
        }

        public KroxyliciousOperatorBuilder addToTheBindingsNamespaces(String bindingsNamespace) {
            this.bindingsNamespaces = new ArrayList<>(Objects.requireNonNullElseGet(this.bindingsNamespaces, () -> Collections.singletonList(bindingsNamespace)));
            return self();
        }

        public KroxyliciousOperatorBuilder withOperationTimeout(Duration operationTimeout) {
            this.operationTimeout = operationTimeout;
            return self();
        }

        public KroxyliciousOperatorBuilder withReconciliationInterval(Duration reconciliationInterval) {
            this.reconciliationInterval = reconciliationInterval;
            return self();
        }

        public KroxyliciousOperatorBuilder withExtraLabels(Map<String, String> extraLabels) {
            this.extraLabels = extraLabels;
            return self();
        }

        public KroxyliciousOperatorBuilder withReplicas(int replicas) {
            this.replicas = replicas;
            return self();
        }

        private KroxyliciousOperatorBuilder self() {
            return this;
        }

        public KroxyliciousOperatorBundleInstaller createBundleInstallation() {
            return new KroxyliciousOperatorBundleInstaller(this);
        }
    }
}
