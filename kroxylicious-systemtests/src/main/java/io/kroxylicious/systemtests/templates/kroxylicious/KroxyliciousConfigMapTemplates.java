/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.io.UncheckedIOException;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
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
     * Kroxylicious record encryption config.
     *
     * @param clusterName the cluster name
     * @param namespaceName the namespace name
     * @param testKmsFacade the test kms facade
     * @return the config map builder
     */
    public static ConfigMapBuilder kroxyliciousRecordEncryptionConfig(String clusterName, String namespaceName, TestKmsFacade<?, ?, ?> testKmsFacade) {
        return baseKroxyliciousConfig(namespaceName)
                .addToData("config.yaml", getRecordEncryptionConfigMap(clusterName, testKmsFacade));
    }

    private static FilterDefinition buildEncryptionFilterDefinition(TestKmsFacade<?, ?, ?> testKmsFacade) {
        return new FilterDefinitionBuilder("RecordEncryption")
                .withConfig("kms", testKmsFacade.getKmsServiceClass().getSimpleName())
                .withConfig("kmsConfig", testKmsFacade.getKmsServiceConfig())
                .withConfig("selector", "TemplateKekSelector")
                .withConfig("selectorConfig", Map.of("template", "${topicName}"))
                .build();
    }

    private static String getRecordEncryptionConfigMap(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        FilterDefinition filterDefinition = buildEncryptionFilterDefinition(testKmsFacade);
        String configYaml;
        try {
            configYaml = mapper.writeValueAsString(filterDefinition).replace("---", "").indent(2).trim();
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }

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
                - %FILTER_CONFIG%
                """
                .replace("%NAMESPACE%", Constants.KAFKA_DEFAULT_NAMESPACE)
                .replace("%CLUSTER_NAME%", clusterName)
                .replace("%KROXY_SERVICE_NAME%", Constants.KROXY_SERVICE_NAME)
                .replace("%FILTER_CONFIG%", configYaml);
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
