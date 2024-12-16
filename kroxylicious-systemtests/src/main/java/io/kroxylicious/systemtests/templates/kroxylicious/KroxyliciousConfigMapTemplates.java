/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.utils.ReadWriteUtils;

/**
 * The type Kroxylicious config templates.
 */
public final class KroxyliciousConfigMapTemplates {

    private KroxyliciousConfigMapTemplates() {
    }

    private static ConfigMapBuilder baseKroxyliciousConfig(String namespaceName) {
        return new ConfigMapBuilder()
                .withApiVersion("v1")
                .withKind(Constants.CONFIG_MAP)
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
     * Kroxylicious record encryption config.
     *
     * @param clusterName the cluster name
     * @param namespaceName the namespace name
     * @param testKmsFacade the test kms facade
     * @return the config map builder
     */
    public static ConfigMapBuilder kroxyliciousRecordEncryptionConfig(String clusterName, String namespaceName, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                                      ExperimentalKmsConfig experimentalKmsConfig) {
        return baseKroxyliciousConfig(namespaceName)
                .addToData("config.yaml", getRecordEncryptionConfigMap(clusterName, testKmsFacade, experimentalKmsConfig));
    }

    private static String buildEncryptionFilter(TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        return """
                - type: RecordEncryption
                  config:
                    kms: %s
                    kmsConfig:
                      %s
                    selector: TemplateKekSelector
                    selectorConfig:
                      template: "KEK_${topicName}"
                    experimental:
                      %s
                """.formatted(testKmsFacade.getKmsServiceClass().getSimpleName(),
                ReadWriteUtils.writeObjectToYamlString(testKmsFacade.getKmsServiceConfig(), 6),
                ReadWriteUtils.writeObjectToYamlString(experimentalKmsConfig, 6));
    }

    private static String getRecordEncryptionConfigMap(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        String configYaml = buildEncryptionFilter(testKmsFacade, experimentalKmsConfig);

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
                        brokerAddressPattern: %s
                    targetCluster:
                      bootstrap_servers: %s-kafka-bootstrap.%s.svc.cluster.local:9092
                    logFrames: false
                filters:
                %s
                """
                .formatted(Constants.KROXY_SERVICE_NAME, clusterName, Constants.KAFKA_DEFAULT_NAMESPACE, configYaml);
    }

    private static String getDefaultKroxyliciousConfigMap(String clusterName) {
        return """
                adminHttp:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  demo:
                    targetCluster:
                      bootstrap_servers: %s-kafka-bootstrap.%s.svc.cluster.local:9092
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: localhost:9292
                        brokerAddressPattern: %s
                    logNetwork: false
                    logFrames: false
                """
                .formatted(clusterName, Constants.KAFKA_DEFAULT_NAMESPACE, Constants.KROXY_SERVICE_NAME);
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
                      bootstrap_servers: %s:9094
                    clusterNetworkAddressConfigProvider:
                      type: PortPerBrokerClusterNetworkAddressConfigProvider
                      config:
                        bootstrapAddress: localhost:9292
                    logNetwork: false
                    logFrames: false
                """
                .formatted(clusterExternalIP);
    }
}
