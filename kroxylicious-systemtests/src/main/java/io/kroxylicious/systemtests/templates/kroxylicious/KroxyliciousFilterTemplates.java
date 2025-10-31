/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import io.kroxylicious.filter.encryption.RecordEncryption;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.systemtests.Constants;
import io.kroxylicious.systemtests.resources.kms.ExperimentalKmsConfig;
import io.kroxylicious.systemtests.resources.kms.azure.KubeLowKeyVaultKmsTestKmsFacade;
import io.kroxylicious.systemtests.utils.DeploymentUtils;

/**
 * The type Kroxylicious filter templates.
 */
public final class KroxyliciousFilterTemplates {
    private static final ObjectMapper OBJECT_MAPPER = createBaseObjectMapper();

    private KroxyliciousFilterTemplates() {
    }

    private static ObjectMapper createBaseObjectMapper() {
        YAMLFactory yamlFactory = new YAMLFactory();
        yamlFactory.configure(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID, false);
        return new ObjectMapper(yamlFactory)
                .setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE)
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
                .setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.ANY)
                .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
                .enable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
                .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY)
                .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
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
        DeploymentUtils.copySecretInNamespace(namespaceName, Constants.KEYSTORE_SECRET_NAME);
        DeploymentUtils.copySecretInNamespace(namespaceName, Constants.TRUSTSTORE_SECRET_NAME);
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

        String kekTemplate = "KEK_$(topicName)";
        if (testKmsFacade instanceof KubeLowKeyVaultKmsTestKmsFacade) {
            kekTemplate = "KEK-$(topicName)";
        }

        var map = new HashMap<>(Map.of(
                "kms", testKmsFacade.getKmsServiceClass().getSimpleName(),
                "kmsConfig", kmsConfigMap,
                "selector", "TemplateKekSelector",
                "selectorConfig", Map.of("template", kekTemplate)));

        if (experimentalKmsConfig != null) {
            Map<String, Object> experimentalKmsConfigMap = OBJECT_MAPPER
                    .convertValue(experimentalKmsConfig, new TypeReference<>() {
                    });
            map.put("experimental", experimentalKmsConfigMap);
        }

        return map;
    }
}
