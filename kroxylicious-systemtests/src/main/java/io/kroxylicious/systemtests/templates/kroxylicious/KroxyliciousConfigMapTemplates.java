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
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;

/**
 * The type Kroxylicious config templates.
 */
public final class KroxyliciousConfigMapTemplates {
    private static final YAMLFactory FACTORY = YAMLFactory.builder()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .build();
    private static final ObjectMapper YAML_OBJECT_MAPPER = new ObjectMapper(FACTORY);

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
                - name: encrypt
                  type: RecordEncryption
                  config:
                    kms: %s
                    kmsConfig:
                      %s
                    selector: TemplateKekSelector
                    selectorConfig:
                      template: "KEK_$(topicName)"
                    experimental:
                      %s
                """.formatted(testKmsFacade.getKmsServiceClass().getSimpleName(), getNestedYaml(testKmsFacade.getKmsServiceConfig(), 6),
                getNestedYaml(experimentalKmsConfig, 6));
    }

    private static String getNestedYaml(Object config, int indent) {
        String configYaml;

        try {
            configYaml = YAML_OBJECT_MAPPER.writeValueAsString(config).indent(indent).trim();
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }

        return configYaml;
    }

    private static String getRecordEncryptionConfigMap(String clusterName, TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        String configYaml = buildEncryptionFilter(testKmsFacade, experimentalKmsConfig);

        return """
                management:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  - name: my-cluster-proxy
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9292
                        advertisedBrokerAddressPattern: %s
                    targetCluster:
                      bootstrapServers: %s-kafka-bootstrap.%s.svc.cluster.local:9092
                    logFrames: false
                filterDefinitions:
                %s
                defaultFilters:
                  - encrypt
                """
                .formatted(Constants.KROXY_SERVICE_NAME, clusterName, Constants.KAFKA_DEFAULT_NAMESPACE, configYaml);
    }

    /**
     * Gets default external kroxylicious config map.
     *
     * @param clusterExternalIP the cluster external ip
     * @return the default external kroxylicious config map
     */
    public static String getDefaultExternalKroxyliciousConfigMap(String clusterExternalIP) {
        return """
                management:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  - name: demo
                    targetCluster:
                      bootstrapServers: %s:9094
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9292
                    logNetwork: false
                    logFrames: false
                """
                .formatted(clusterExternalIP);
    }
}
