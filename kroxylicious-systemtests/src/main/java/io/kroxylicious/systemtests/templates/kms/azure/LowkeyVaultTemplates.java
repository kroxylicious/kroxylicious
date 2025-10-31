/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kms.azure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrStringBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.templates.ContainerTemplates;

/**
 * The type Lowkey vault templates.
 */
public class LowkeyVaultTemplates {
    private static final String LOWKEY_VAULT_NAMES_VAR = "LOWKEY_VAULT_NAMES";
    private static final String LOWKEY_VAULT_ALIASES_VAR = "LOWKEY_VAULT_ALIASES";
    private static final int CLUSTER_IP_PORT = 443;

    private static List<ContainerPort> getLowKeyVaultContainerPorts() {
        List<ContainerPort> containerPorts = new ArrayList<>();
        containerPorts.add(getContainerPort(8080));
        containerPorts.add(getContainerPort(8443));
        return containerPorts;
    }

    private static ContainerPort getContainerPort(int port) {
        return new ContainerPortBuilder()
                .withContainerPort(port)
                .build();
    }

    /**
     * Base service builder.
     *
     * @param serviceName the service name
     * @param namespace the namespace
     * @param servicePorts the service ports
     * @param serviceType the service type
     * @return  the service builder
     */
    public static ServiceBuilder baseService(String serviceName, String namespace, String selector, List<ServicePort> servicePorts, String serviceType) {
        Map<String, String> labelSelector = Map.of("app", selector);
        // @formatter:off
        return new ServiceBuilder()
                .withKind(Constants.SERVICE)
                .withNewMetadata()
                .withName(serviceName)
                .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                .withPorts(servicePorts)
                .withType(serviceType)
                .withSelector(labelSelector)
                .endSpec();
        // @formatter:on
    }

    private static List<ServicePort> getLowkeyVaultNodePortPorts() {
        List<ServicePort> servicePorts = new ArrayList<>();
        servicePorts.add(getPort(8080, 8080, "http"));
        servicePorts.add(getPort(8443, 8443, "https"));
        return servicePorts;
    }

    private static List<ServicePort> getLowkeyVaultClusterIPPorts() {
        List<ServicePort> servicePorts = new ArrayList<>();
        servicePorts.add(getPort(CLUSTER_IP_PORT, 8443, "https"));
        return servicePorts;
    }

    private static ServicePort getPort(int port, int targetPort, String name) {
        return new ServicePortBuilder()
                .withPort(port)
                .withName(name)
                .withProtocol("TCP")
                .withTargetPort(new IntOrStringBuilder().withValue(targetPort).build())
                .build();
    }

    private static EnvVar envVar(String name, String value) {
        return new EnvVarBuilder()
                .withName(name)
                .withValue(value)
                .build();
    }

    private static List<EnvVar> lowkeyVaultEnvVars(String serviceName, String namespace, String nodePortEndpoint, String vaultName, String certFilePath,
                                                   String password) {
        String fqdnEndpoint = serviceName + "-" + Constants.CLUSTER_IP_TYPE.toLowerCase() + "." + namespace + ".svc.cluster.local:" + CLUSTER_IP_PORT;
        return new ArrayList<>(List.of(
                envVar("server.ssl.key-store-type", "JKS"),
                envVar("server.ssl.key-store", certFilePath),
                envVar("server.ssl.key-store-password", password),
                envVar(LOWKEY_VAULT_NAMES_VAR, vaultName),
                envVar(LOWKEY_VAULT_ALIASES_VAR, "localhost=" + nodePortEndpoint + ",localhost=" + fqdnEndpoint)));
    }

    /**
     * Default mock oauth server deployment builder.
     *
     * @param deploymentName the deployment name
     * @param image the image
     * @param namespace the namespace
     * @return  the deployment builder
     */
    public static DeploymentBuilder defaultMockOauthServerDeployment(String deploymentName, String image, String namespace) {
        Map<String, String> labelSelector = Map.of("app", deploymentName);
        // @formatter:off
        return new DeploymentBuilder()
                .withKind(Constants.DEPLOYMENT)
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(deploymentName)
                    .withLabels(labelSelector)
                .endMetadata()
                .withNewSpec()
                    .withNewSelector()
                        .addToMatchLabels(labelSelector)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(labelSelector)
                        .endMetadata()
                        .withNewSpec()
                            .withContainers(ContainerTemplates.baseImageBuilder(deploymentName, image)
                                    .withPorts(getOauthServerContainerPorts())
                                    .build())
                        .endSpec()
                    .endTemplate()
                .endSpec();
        // @formatter:on
    }

    private static List<ContainerPort> getOauthServerContainerPorts() {
        List<ContainerPort> containerPorts = new ArrayList<>();
        containerPorts.add(getContainerPort(8080));
        return containerPorts;
    }

    /**
     * Default mock oauth server service builder.
     *
     * @param serviceName the service name
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultMockOauthServerService(String serviceName, String namespace, String selector) {
        return baseService(serviceName, namespace, selector, getOauthServerPorts(), "NodePort");
    }

    private static List<ServicePort> getOauthServerPorts() {
        List<ServicePort> servicePorts = new ArrayList<>();
        servicePorts.add(getPort(8080, 8080, "http"));
        return servicePorts;
    }

    private static List<ServicePort> getOauthServerClusterIPPorts() {
        List<ServicePort> servicePorts = new ArrayList<>();
        servicePorts.add(getPort(80, 8080, "http"));
        return servicePorts;
    }

    /**
     * Default lowkey vault deployment builder.
     *
     * @param deploymentName the deployment name
     * @param image the image
     * @param namespace the namespace
     * @param endpoint the endpoint
     * @return  the deployment builder
     */
    public static DeploymentBuilder defaultLowkeyVaultDeployment(String deploymentName, String image, String namespace, String endpoint, String password) {
        Map<String, String> labelSelector = Map.of("app", deploymentName);
        // @formatter:off
        return new DeploymentBuilder()
                .withKind(Constants.DEPLOYMENT)
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(deploymentName)
                    .withLabels(labelSelector)
                .endMetadata()
                .withNewSpec()
                    .withNewSelector()
                        .addToMatchLabels(labelSelector)
                    .endSelector()
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(labelSelector)
                        .endMetadata()
                        .withNewSpec()
                            .withContainers(ContainerTemplates.baseImageBuilder(deploymentName, image)
                                    .withEnv(lowkeyVaultEnvVars("lowkey-vault", namespace, endpoint, deploymentName, Constants.KEYSTORE_TEMP_DIR + Constants.KEYSTORE_FILE_NAME, password))
                                    .withPorts(getLowKeyVaultContainerPorts())
                                    .withVolumeMounts(new VolumeMountBuilder()
                                            .withName("cert")
                                            .withMountPath(Constants.KEYSTORE_TEMP_DIR)
                                            .withReadOnly(true)
                                            .build())
                                    .build())
                            .withVolumes(new VolumeBuilder()
                                    .withSecret(new SecretVolumeSourceBuilder()
                                            .withSecretName(Constants.KEYSTORE_SECRET_NAME)
                                            .build())
                                    .withName("cert")
                                    .build())
                        .endSpec()
                    .endTemplate()
                .endSpec();
        // @formatter:on
    }

    /**
     * Default lowkey vault service builder.
     *
     * @param serviceName the service name
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultLowkeyVaultNodePortService(String serviceName, String namespace, String selector) {
        return baseService(serviceName, namespace, selector, getLowkeyVaultNodePortPorts(), Constants.NODE_PORT_TYPE);
    }

    /**
     * Default lowkey vault cluster ip service builder.
     *
     * @param serviceName the service name
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultLowkeyVaultClusterIPService(String serviceName, String namespace, String selector) {
        return baseService(serviceName, namespace, selector, getLowkeyVaultClusterIPPorts(), Constants.CLUSTER_IP_TYPE);
    }

    public static ServiceBuilder defaultMockOauthServerClusterIPService(String serviceName, String namespace, String selector) {
        return baseService(serviceName, namespace, selector, getOauthServerClusterIPPorts(), Constants.CLUSTER_IP_TYPE);
    }
}
