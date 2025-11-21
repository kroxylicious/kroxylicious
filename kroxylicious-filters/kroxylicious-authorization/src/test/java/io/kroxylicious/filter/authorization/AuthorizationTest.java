/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

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
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.mockito.ArgumentMatchers.any;
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