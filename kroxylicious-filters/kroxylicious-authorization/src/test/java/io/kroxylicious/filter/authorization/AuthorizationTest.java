/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.AuthorizerService;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.test.schema.SchemaValidationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthorizationTest {

    @Mock
    FilterFactoryContext filterFactoryContext;

    @Mock
    AuthorizationConfig authorizationConfig;

    @Mock
    AuthorizerService authzService;

    @Mock
    Authorizer authz;

    @Mock
    ResolvedPluginRegistry resolvedPluginRegistry;

    @Test
    void shouldInitializeResourceTypesAreASubsetOfSupportedResourceTypes() {
        // Given
        Authorization authorization = new Authorization();
        when(filterFactoryContext.pluginInstance(any(), any())).thenReturn(authzService);
        when(authzService.build()).thenReturn(authz);
        when(authz.supportedResourceTypes()).thenReturn(Optional.of(Set.of()));

        // Then
        Assertions.assertThatNoException().isThrownBy(() -> authorization.initialize(filterFactoryContext, authorizationConfig));
    }

    @Test
    void shouldFailToInitializeWhenUnknownResourceType() {
        // Given
        Authorization authorization = new Authorization();
        when(authorizationConfig.authorizer()).thenReturn("Wibble");
        when(filterFactoryContext.pluginInstance(any(), any())).thenReturn(authzService);
        when(authzService.build()).thenReturn(authz);
        when(authz.supportedResourceTypes()).thenReturn(Optional.of(Set.of(Unexpected.class)));

        // Then
        Assertions.assertThatThrownBy(() -> authorization.initialize(filterFactoryContext, authorizationConfig))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessage("Wibble specifies access controls for resource types which cannot be enforced by this filter. "
                        + "The unsupported types are: io.kroxylicious.filter.authorization.Unexpected.");
    }

    @Test
    void shouldHaveLegacyAndConfig2PluginAnnotations() {
        Plugin[] annotations = Authorization.class.getAnnotationsByType(Plugin.class);

        assertThat(annotations).hasSize(2);

        var versionToConfigType = java.util.Arrays.stream(annotations)
                .collect(java.util.stream.Collectors.toMap(Plugin::configVersion, Plugin::configType));

        assertThat(versionToConfigType).containsOnly(
                entry("", AuthorizationConfig.class),
                entry("v1alpha1", AuthorizationConfigV1.class));
    }

    @Test
    void fullConfigShouldPassSchemaValidation() {
        // Config with all fields populated that Java accepts
        new AuthorizationConfigV1("my-authorizer");

        // Same config in raw YAML form
        SchemaValidationAssert.assertSchemaAccepts("Authorization", "v1alpha1", Map.of(
                "authorizer", "my-authorizer"));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void shouldInitializeWithV1Config() {
        // Given
        Authorization authorization = new Authorization();
        var v1Config = new AuthorizationConfigV1("my-authorizer");
        when(filterFactoryContext.resolvedPluginRegistry()).thenReturn(Optional.of(resolvedPluginRegistry));
        when(resolvedPluginRegistry.pluginInstance(AuthorizerService.class, "my-authorizer")).thenReturn(authzService);
        when(resolvedPluginRegistry.pluginConfig(
                eq("io.kroxylicious.authorizer.service.AuthorizerService"), eq("my-authorizer")))
                .thenReturn(Map.of("aclFile", "/path/to/rules.txt"));
        when(authzService.build()).thenReturn(authz);
        when(authz.supportedResourceTypes()).thenReturn(Optional.of(Set.of()));

        // When/Then
        Assertions.assertThatNoException().isThrownBy(() -> authorization.initialize(filterFactoryContext, v1Config));
    }

    @Test
    void shouldRejectV1ConfigWithoutRegistry() {
        // Given
        Authorization authorization = new Authorization();
        var v1Config = new AuthorizationConfigV1("my-authorizer");

        // When/Then
        Assertions.assertThatThrownBy(() -> authorization.initialize(filterFactoryContext, v1Config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("ResolvedPluginRegistry");
    }

    @Test
    void shouldCloseAuthorizer() {
        // Given
        Authorization authorization = new Authorization();
        when(authorizationConfig.authorizer()).thenReturn("Wibble");
        when(filterFactoryContext.pluginInstance(any(), any())).thenReturn(authzService);
        when(authzService.build()).thenReturn(authz);
        when(authz.supportedResourceTypes()).thenReturn(Optional.empty());
        authorization.initialize(filterFactoryContext, authorizationConfig);

        // When
        authorization.close(authz);

        // Then
        Mockito.verify(authzService).close();
    }

}
