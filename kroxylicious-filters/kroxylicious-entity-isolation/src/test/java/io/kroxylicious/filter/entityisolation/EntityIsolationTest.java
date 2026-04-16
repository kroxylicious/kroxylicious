/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterDispatchExecutor;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.test.schema.SchemaValidationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class EntityIsolationTest {
    @Test
    @SuppressWarnings({ "unchecked", "resource" })
    void shouldInitAndCreateFilter() {
        var config = new EntityIsolation.Config(Set.of(EntityIsolation.EntityType.GROUP_ID), "MAPPER", null);
        var entityIsolation = new EntityIsolation();
        var fc = mock(FilterFactoryContext.class);
        var mapperService = mock(EntityNameMapperService.class);
        var mapper = mock(EntityNameMapper.class);

        doReturn(mapperService).when(fc).pluginInstance(EntityNameMapperService.class, "MAPPER");
        doReturn(mapper).when(mapperService).build();
        doReturn(mock(FilterDispatchExecutor.class)).when(fc).filterDispatchExecutor();

        var sec = entityIsolation.initialize(fc, config);
        var filter = entityIsolation.createFilter(fc, sec);
        assertThat(filter).isNotNull();
        verify(mapperService).initialize(null);
        entityIsolation.close(sec);
    }

    @Test
    void topicMappingNotYetSupported() {
        var topicName = Set.of(EntityIsolation.EntityType.TOPIC_NAME);
        assertThatThrownBy(() -> new EntityIsolation.Config(topicName, "MAPPER", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldHaveLegacyAndConfig2PluginAnnotations() {
        Plugin[] annotations = EntityIsolation.class.getAnnotationsByType(Plugin.class);

        assertThat(annotations).hasSize(2);

        var versionToConfigType = java.util.Arrays.stream(annotations)
                .collect(java.util.stream.Collectors.toMap(Plugin::configVersion, Plugin::configType));

        assertThat(versionToConfigType).containsOnly(
                entry("", EntityIsolation.Config.class),
                entry("v1alpha1", EntityIsolationConfigV1.class));
    }

    @Test
    void fullConfigShouldPassSchemaValidation() {
        // Config with all fields populated that Java accepts
        new EntityIsolationConfigV1(
                Set.of(EntityIsolation.EntityType.GROUP_ID, EntityIsolation.EntityType.TRANSACTIONAL_ID),
                "my-mapper");

        // Same config in raw YAML form
        SchemaValidationAssert.assertSchemaAccepts("EntityIsolation", "v1alpha1", Map.of(
                "entityTypes", java.util.List.of("GROUP_ID", "TRANSACTIONAL_ID"),
                "mapper", "my-mapper"));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void shouldInitAndCreateFilterWithV1Config() {
        var v1Config = new EntityIsolationConfigV1(
                Set.of(EntityIsolation.EntityType.GROUP_ID),
                "my-mapper");
        var entityIsolation = new EntityIsolation();
        var fc = mock(FilterFactoryContext.class);
        var registry = mock(ResolvedPluginRegistry.class);
        var mapperService = mock(EntityNameMapperService.class);
        var mapper = mock(EntityNameMapper.class);

        doReturn(Optional.of(registry)).when(fc).resolvedPluginRegistry();
        doReturn(mapperService).when(registry).pluginInstance(EntityNameMapperService.class, "my-mapper");
        doReturn(Map.of("someKey", "someValue")).when(registry).pluginConfig(
                eq("io.kroxylicious.filter.entityisolation.EntityNameMapperService"), eq("my-mapper"));
        doReturn(mapper).when(mapperService).build();
        doReturn(mock(FilterDispatchExecutor.class)).when(fc).filterDispatchExecutor();

        var sec = entityIsolation.initialize(fc, v1Config);
        var filter = entityIsolation.createFilter(fc, sec);
        assertThat(filter).isNotNull();
    }

    @Test
    void shouldRejectV1ConfigWithoutRegistry() {
        var v1Config = new EntityIsolationConfigV1(
                Set.of(EntityIsolation.EntityType.GROUP_ID),
                "my-mapper");
        var entityIsolation = new EntityIsolation();
        var fc = mock(FilterFactoryContext.class);

        assertThatThrownBy(() -> entityIsolation.initialize(fc, v1Config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("ResolvedPluginRegistry");
    }

}
