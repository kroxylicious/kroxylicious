/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.filter.encryption.RecordEncryption;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;

/**
 * The type Kroxylicious filter templates.
 */
public final class KroxyliciousFilterTemplates {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KroxyliciousFilterTemplates() {
    }

    /**
     * Default kafka cluster ref deployment.
     *
     * @param namespaceName the namespace name
     * @param filterName the filter name
     * @return the kafka service builder
     */
    public static KafkaProtocolFilterBuilder baseFilterDeployment(String namespaceName, String filterName) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withName(filterName)
                    .withNamespace(namespaceName)
                .endMetadata()
                    .withNewSpec()
                .endSpec();
        // @formatter:on
    }

    /**
     * Kroxylicious record encryption filter.
     *
     * @param namespaceName the namespace name
     * @param testKmsFacade the test kms facade
     * @param experimentalKmsConfig the experimental kms config
     * @return the kafka protocol filter builder
     */
    public static KafkaProtocolFilterBuilder kroxyliciousRecordEncryptionFilter(String namespaceName, TestKmsFacade<?, ?, ?> testKmsFacade,
                                                                                ExperimentalKmsConfig experimentalKmsConfig) {
        return baseFilterDeployment(namespaceName, Constants.KROXYLICIOUS_ENCRYPTION_FILTER_NAME)
                .withNewSpec()
                .withType(RecordEncryption.class.getTypeName())
                .withConfigTemplate(getRecordEncryptionConfigMap(testKmsFacade, experimentalKmsConfig))
                .endSpec();
    }

    private static Map<String, Object> getRecordEncryptionConfigMap(TestKmsFacade<?, ?, ?> testKmsFacade, ExperimentalKmsConfig experimentalKmsConfig) {
        Map<String, Object> kmsConfigMap = OBJECT_MAPPER
                .convertValue(testKmsFacade.getKmsServiceConfig(), new TypeReference<>() {
                });

        var map = new HashMap<>(Map.of(
                "kms", testKmsFacade.getKmsServiceClass().getSimpleName(),
                "kmsConfig", kmsConfigMap,
                "selector", "TemplateKekSelector",
                "selectorConfig", Map.of("template", "KEK_$(topicName)")));

        if (experimentalKmsConfig != null) {
            Map<String, Object> experimentalKmsConfigMap = OBJECT_MAPPER
                    .convertValue(experimentalKmsConfig, new TypeReference<>() {
                    });
            map.put("experimental", experimentalKmsConfigMap);
        }

        return map;
    }
}
