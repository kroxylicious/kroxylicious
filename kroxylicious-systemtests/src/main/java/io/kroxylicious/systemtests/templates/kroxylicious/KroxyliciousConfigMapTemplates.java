/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.systemtests.Constants;

/**
 * The type Kroxylicious config templates.
 */
public final class KroxyliciousConfigMapTemplates {

    private KroxyliciousConfigMapTemplates() {
    }

    private static ConfigMapBuilder baseKroxyliciousConfig(String namespaceName) {
        return new ConfigMapBuilder()
                .withApiVersion("v1")
                .withKind(Constants.CONFIG_MAP_KIND)
                .editMetadata()
                .withName(Constants.KROXY_CONFIG_NAME)
                .withNamespace(namespaceName)
                .endMetadata();
    }

    /**
     * Default kroxylicious config map builder.
     *
     * @param clusterName the cluster name
     * @param namespaceName the namespace name
     * @return the config map builder
     */
    public static ConfigMapBuilder defaultKroxyliciousConfig(String clusterName, String namespaceName) {
        return baseKroxyliciousConfig(namespaceName)
                .addToData("config.yaml", getDefaultKroxyliciousConfigMap(clusterName));
    }

    /**
     * Kroxylicious topic encryption config.
     *
     * @param clusterName the cluster name
     * @param namespaceName the namespace name
     * @param topicName the topic name
     * @param config config
     * @return the config map builder
     */
    public static ConfigMapBuilder kroxyliciousRecordEncryptionConfig(String clusterName, String namespaceName, String topicName, Config config) {
        return baseKroxyliciousConfig(namespaceName)
                .addToData("config.yaml", getRecordEncryptionConfigMap(clusterName, topicName, config));
    }

    private static String getRecordEncryptionConfigMap(String clusterName, String topicName, Config config) {
        return """
                adminHttp:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  my-cluster-proxy:
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: localhost:9292
                        brokerAddressPattern: %KROXY_SERVICE_NAME%
                    targetCluster:
                      bootstrap_servers: %CLUSTER_NAME%-kafka-bootstrap.%NAMESPACE%.svc.cluster.local:9092
                    logFrames: false
                filters:
                - type: EnvelopeEncryption
                  config:
                    kms: VaultKmsService
                    kmsConfig:
                      vaultTransitEngineUrl: %VAULT_TRANSIT_ENGINE_URL%
                      vaultToken:
                        password: %VAULT_TOKEN%
                    selector: TemplateKekSelector
                    selectorConfig:
                      template: "${topicName}"
                """
                .replace("%NAMESPACE%", Constants.KAFKA_DEFAULT_NAMESPACE)
                .replace("%CLUSTER_NAME%", clusterName)
                .replace("%KROXY_SERVICE_NAME%", Constants.KROXY_SERVICE_NAME)
                .replace("%VAULT_TOKEN%", config.vaultToken().getProvidedPassword())
                .replace("%VAULT_TRANSIT_ENGINE_URL%", config.vaultTransitEngineUrl().toString())
                .replace("%TOPIC_NAME%", topicName);
    }

    private static String getDefaultKroxyliciousConfigMap(String clusterName) {
        return """
                adminHttp:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  demo:
                    targetCluster:
                      bootstrap_servers: %CLUSTER_NAME%-kafka-bootstrap.%NAMESPACE%.svc.cluster.local:9092
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: localhost:9292
                        brokerAddressPattern: %KROXY_SERVICE_NAME%
                    logNetwork: false
                    logFrames: false
                """
                .replace("%NAMESPACE%", Constants.KAFKA_DEFAULT_NAMESPACE)
                .replace("%CLUSTER_NAME%", clusterName)
                .replace("%KROXY_SERVICE_NAME%", Constants.KROXY_SERVICE_NAME);
    }

    /**
     * Gets default external kroxylicious config map.
     *
     * @param clusterExternalIP the cluster external ip
     * @return the default external kroxylicious config map
     */
    public static String getDefaultExternalKroxyliciousConfigMap(String clusterExternalIP) {
        return """
                adminHttp:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  demo:
                    targetCluster:
                      bootstrap_servers: %CLUSTER_EXTERNAL_IP%:9094
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: localhost:9292
                    logNetwork: false
                    logFrames: false
                """
                .replace("%CLUSTER_EXTERNAL_IP%", clusterExternalIP);
    }
}
