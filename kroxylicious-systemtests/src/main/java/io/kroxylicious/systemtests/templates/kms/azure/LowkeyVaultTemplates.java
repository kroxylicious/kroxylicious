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
import io.kroxylicious.systemtests.utils.DeploymentUtils;

/**
 * The type Lowkey vault templates.
 */
public class LowkeyVaultTemplates {
    private static final String LOWKEY_VAULT_NAMES_VAR = "LOWKEY_VAULT_NAMES";
    private static final String LOWKEY_VAULT_ALIASES_VAR = "LOWKEY_VAULT_ALIASES";
    public static final String LOWKEY_VAULT_NODE_PORT_SERVICE_NAME = "lowkey-vault-" + Constants.NODE_PORT_TYPE.toLowerCase();
    public static final String LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME = "lowkey-vault-" + Constants.CLUSTER_IP_TYPE.toLowerCase();
    public static final String MOCK_OAUTH_SERVER_NODE_PORT_SERVICE_NAME = "mock-oauth2-server-" + Constants.NODE_PORT_TYPE.toLowerCase();
    public static final String MOCK_OAUTH_SERVER_CLUSTER_IP_SERVICE_NAME = "mock-oauth2-server-" + Constants.CLUSTER_IP_TYPE.toLowerCase();
    private static final String LOWKEY_VAULT_DEPLOYMENT_NAME = "my-key-vault";
    public static final String MOCK_OAUTH_SERVER_SERVICE_NAME = "mock-oauth2-server";
    private static final int CLUSTER_IP_PORT = 443;

    private LowkeyVaultTemplates() {

    }

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

    private static List<EnvVar> lowkeyVaultEnvVars(String namespace, String nodePortEndpoint, String certFilePath, String password) {
        String fqdnEndpoint = LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME + "." + namespace + ".svc.cluster.local:" + CLUSTER_IP_PORT;
        return new ArrayList<>(List.of(
                envVar("server.ssl.key-store-type", "JKS"),
                envVar("server.ssl.key-store", certFilePath),
                envVar("server.ssl.key-store-password", password),
                envVar(LOWKEY_VAULT_NAMES_VAR, LOWKEY_VAULT_DEPLOYMENT_NAME),
                envVar(LOWKEY_VAULT_ALIASES_VAR, "localhost=" + nodePortEndpoint + ",localhost=" + fqdnEndpoint)));
    }

    /**
     * Default mock oauth server deployment builder.
     *
     * @param image the image
     * @param namespace the namespace
     * @return  the deployment builder
     */
    public static DeploymentBuilder defaultMockOauthServerDeployment(String image, String namespace) {
        Map<String, String> labelSelector = Map.of("app", MOCK_OAUTH_SERVER_SERVICE_NAME);
        String certPassword = DeploymentUtils.getSecretValue(namespace, Constants.KEYSTORE_SECRET_NAME, "password");

        // @formatter:off
        return new DeploymentBuilder()
                .withKind(Constants.DEPLOYMENT)
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(MOCK_OAUTH_SERVER_SERVICE_NAME)
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
                            .withContainers(ContainerTemplates.baseImageBuilder(MOCK_OAUTH_SERVER_SERVICE_NAME, image)
                                    .withPorts(getOauthServerContainerPorts())
                                    .withEnv(mockOauth2ServerEnvVars(certPassword))
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

    private static List<EnvVar> mockOauth2ServerEnvVars(String certPassword) {
        String certFilePath = Constants.KEYSTORE_TEMP_DIR + Constants.KEYSTORE_FILE_NAME;
        String keystoreTpe = "JKS";
        String config = """
                {
                    "httpServer" : {
                        "type" : "NettyWrapper",
                        "ssl" : {
                            "keyPassword" : "%s",
                            "keystoreFile" : "%s",
                            "keystoreType" : "%s",
                            "keystorePassword" : "%s"
                        }
                    }
                }
                """.formatted(certPassword, certFilePath, keystoreTpe, certPassword);

        return List.of(envVar("JSON_CONFIG", config));
    }

    private static List<ContainerPort> getOauthServerContainerPorts() {
        List<ContainerPort> containerPorts = new ArrayList<>();
        containerPorts.add(getContainerPort(8080));
        return containerPorts;
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
     * @param image the image
     * @param namespace the namespace
     * @param endpoint the endpoint
     * @return  the deployment builder
     */
    public static DeploymentBuilder defaultLowkeyVaultDeployment(String image, String namespace, String endpoint) {
        Map<String, String> labelSelector = Map.of("app", LOWKEY_VAULT_DEPLOYMENT_NAME);
        String password = DeploymentUtils.getSecretValue(namespace, Constants.KEYSTORE_SECRET_NAME, "password");

        // @formatter:off
        return new DeploymentBuilder()
                .withKind(Constants.DEPLOYMENT)
                .withNewMetadata()
                    .withNamespace(namespace)
                    .withName(LOWKEY_VAULT_DEPLOYMENT_NAME)
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
                            .withContainers(ContainerTemplates.baseImageBuilder(LOWKEY_VAULT_DEPLOYMENT_NAME, image)
                                    .withEnv(lowkeyVaultEnvVars(namespace, endpoint, Constants.KEYSTORE_TEMP_DIR + Constants.KEYSTORE_FILE_NAME, password))
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
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultLowkeyVaultNodePortService(String namespace) {
        return baseService(LOWKEY_VAULT_NODE_PORT_SERVICE_NAME, namespace, LOWKEY_VAULT_DEPLOYMENT_NAME, getLowkeyVaultNodePortPorts(), Constants.NODE_PORT_TYPE);
    }

    /**
     * Default lowkey vault cluster ip service builder.
     *
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultLowkeyVaultClusterIPService(String namespace) {
        return baseService(LOWKEY_VAULT_CLUSTER_IP_SERVICE_NAME, namespace, LOWKEY_VAULT_DEPLOYMENT_NAME, getLowkeyVaultClusterIPPorts(), Constants.CLUSTER_IP_TYPE);
    }

    /**
     * Default mock oauth server service builder.
     *
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultMockOauthServerService(String namespace) {
        return baseService(MOCK_OAUTH_SERVER_NODE_PORT_SERVICE_NAME, namespace, MOCK_OAUTH_SERVER_SERVICE_NAME, getOauthServerPorts(), Constants.NODE_PORT_TYPE);
    }

    /**
     * Default mock oauth server cluster ip service builder.
     *
     * @param namespace the namespace
     * @return  the service builder
     */
    public static ServiceBuilder defaultMockOauthServerClusterIPService(String namespace) {
        return baseService(MOCK_OAUTH_SERVER_CLUSTER_IP_SERVICE_NAME, namespace, MOCK_OAUTH_SERVER_SERVICE_NAME, getOauthServerClusterIPPorts(),
                Constants.CLUSTER_IP_TYPE);
    }
}
