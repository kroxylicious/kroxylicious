/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.io.UncheckedIOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.systemtests.Constants;

/**
 * The type Kroxylicious config templates.
 */
public final class KroxyliciousConfigMapTemplates {
    private static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

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

    private static String buildEncryptionFilter(TestKmsFacade<?, ?, ?> testKmsFacade) {
        return "- type: RecordEncryption"
                + "\n  config:"
                + "\n    kms: " + testKmsFacade.getKmsServiceClass().getSimpleName()
                + "\n    kmsConfig:"
                + "\n      " + getYamlKmsConfig(testKmsFacade.getKmsServiceConfig())
                + "\n    selector: TemplateKekSelector"
                + "\n    selectorConfig:"
                + "\n      template: \"${topicName}\"";
    }

    private static String getYamlKmsConfig(Object config) {
        String configYaml;
        try {
            configYaml = YAML_OBJECT_MAPPER.writeValueAsString(config).replace("---", "").indent(6).trim();
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }

        return configYaml;
    }

    private static String getRecordEncryptionConfigMap(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade) {
        String configYaml = buildEncryptionFilter(testKmsFacade);

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
                %FILTER_CONFIG%
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
