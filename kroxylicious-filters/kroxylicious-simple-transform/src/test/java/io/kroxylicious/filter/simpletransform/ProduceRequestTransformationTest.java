/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.simpletransform;

import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.test.schema.SchemaValidationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link ProduceRequestTransformation} filter factory.
 */
@ExtendWith(MockitoExtension.class)
class ProduceRequestTransformationTest {

    @Mock
    FilterFactoryContext filterFactoryContext;

    @Mock
    ResolvedPluginRegistry resolvedPluginRegistry;

    @Mock
    ByteBufferTransformationFactory<?> transformationFactory;

    @Test
    void shouldHaveLegacyAndConfig2PluginAnnotations() {
        Plugin[] annotations = ProduceRequestTransformation.class.getAnnotationsByType(Plugin.class);

        assertThat(annotations).hasSize(2);

        var versionToConfigType = java.util.Arrays.stream(annotations)
                .collect(java.util.stream.Collectors.toMap(Plugin::configVersion, Plugin::configType));

        assertThat(versionToConfigType).containsOnly(
                entry("", ProduceRequestTransformation.Config.class),
                entry("v1alpha1", ProduceRequestTransformationConfigV1.class));
    }

    @Test
    void fullConfigShouldPassSchemaValidation() {
        new ProduceRequestTransformationConfigV1("my-transform");

        SchemaValidationAssert.assertSchemaAccepts("ProduceRequestTransformation", "v1alpha1", Map.of(
                "transformation", "my-transform"));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void shouldInitializeWithV1Config() {
        var factory = new ProduceRequestTransformation();
        var v1Config = new ProduceRequestTransformationConfigV1("my-transform");

        when(filterFactoryContext.resolvedPluginRegistry()).thenReturn(Optional.of(resolvedPluginRegistry));
        when(resolvedPluginRegistry.pluginInstance(ByteBufferTransformationFactory.class, "my-transform"))
                .thenReturn(transformationFactory);
        when(resolvedPluginRegistry.pluginConfig(
                eq("io.kroxylicious.filter.simpletransform.ByteBufferTransformationFactory"), eq("my-transform")))
                .thenReturn(Map.of("find", "foo", "replace", "bar"));

        var config = factory.initialize(filterFactoryContext, v1Config);
        assertThat(config).isNotNull();
        assertThat(config.transformation()).isEqualTo("my-transform");
    }

    @Test
    void shouldRejectV1ConfigWithoutRegistry() {
        var factory = new ProduceRequestTransformation();
        var v1Config = new ProduceRequestTransformationConfigV1("my-transform");

        assertThatThrownBy(() -> factory.initialize(filterFactoryContext, v1Config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("ResolvedPluginRegistry");
    }
}
