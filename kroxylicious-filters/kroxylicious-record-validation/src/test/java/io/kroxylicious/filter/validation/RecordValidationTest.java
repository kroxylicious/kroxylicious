/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.validation.config.BytebufValidation;
import io.kroxylicious.filter.validation.config.RecordValidationRule;
import io.kroxylicious.filter.validation.config.SyntacticallyCorrectJsonConfig;
import io.kroxylicious.filter.validation.config.TopicMatchingRecordValidationRule;
import io.kroxylicious.filter.validation.config.ValidationConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.test.schema.SchemaValidationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class RecordValidationTest {

    @Test
    void shouldHaveLegacyAndConfig2PluginAnnotations() {
        Plugin[] annotations = RecordValidation.class.getAnnotationsByType(Plugin.class);

        assertThat(annotations).hasSize(2);

        var versionToConfigType = java.util.Arrays.stream(annotations)
                .collect(java.util.stream.Collectors.toMap(Plugin::configVersion, Plugin::configType));

        assertThat(versionToConfigType).containsOnly(
                entry("", ValidationConfig.class),
                entry("v1alpha1", ValidationConfig.class));
    }

    @Test
    void fullConfigShouldPassSchemaValidation() {
        // Config with all fields populated that Java accepts
        new ValidationConfig(
                List.of(new TopicMatchingRecordValidationRule(
                        java.util.Set.of("my-topic"),
                        new BytebufValidation(
                                new SyntacticallyCorrectJsonConfig(true),
                                null, null, true, false),
                        new BytebufValidation(
                                new SyntacticallyCorrectJsonConfig(false),
                                null, null, false, true))),
                new RecordValidationRule(null,
                        new BytebufValidation(null, null, null, true, false)));

        // Same config in raw YAML form
        SchemaValidationAssert.assertSchemaAccepts("RecordValidation", "v1alpha1", Map.of(
                "rules", List.of(Map.of(
                        "topicNames", List.of("my-topic"),
                        "keyRule", Map.of(
                                "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", true),
                                "allowNulls", true,
                                "allowEmpty", false),
                        "valueRule", Map.of(
                                "syntacticallyCorrectJson", Map.of("validateObjectKeysUnique", false),
                                "allowNulls", false,
                                "allowEmpty", true))),
                "defaultRule", Map.of(
                        "valueRule", Map.of(
                                "allowNulls", true,
                                "allowEmpty", false))));
    }

    @Test
    void initRejectsMissingConfig() {
        RecordValidation factory = new RecordValidation();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage(RecordValidation.class.getSimpleName() + " requires configuration, but config object is null");
    }

    @Test
    void shouldInitAndCreateFilter() {
        RecordValidation factory = new RecordValidation();
        var config = factory.initialize(null, new ValidationConfig(List.of(), new RecordValidationRule(null, null)));
        Filter filter = factory.createFilter(null, config);
        assertThat(filter).isNotNull().isInstanceOf(RecordValidationFilter.class);
    }

}
