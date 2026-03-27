/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.termination;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStore;
import io.kroxylicious.sasl.credentialstore.ScramCredentialStoreService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SaslTermination} filter factory.
 */
class SaslTerminationTest {

    @Test
    void shouldCloseCredentialStoreServiceOnFactoryClose() throws Exception {
        // Given
        var factory = new SaslTermination();
        var filterFactoryContext = mock(FilterFactoryContext.class);

        @SuppressWarnings("unchecked")
        var credentialStoreService = mock(ScramCredentialStoreService.class);
        var credentialStore = mock(ScramCredentialStore.class);

        var mechanismConfig = new MechanismConfig(
                "TestCredentialStore",
                Map.of("test", "config"));

        var config = new SaslTerminationConfig(
                Map.of("SCRAM-SHA-256", mechanismConfig));

        when(filterFactoryContext.pluginInstance(
                eq(ScramCredentialStoreService.class),
                eq("TestCredentialStore")))
                .thenReturn(credentialStoreService);

        when(credentialStoreService.buildCredentialStore())
                .thenReturn(credentialStore);

        var context = factory.initialize(filterFactoryContext, config);

        // When
        factory.close(context);

        // Then
        verify(credentialStoreService).close();
    }

    @Test
    void shouldCloseAllCredentialStoreServicesOnFactoryClose() throws Exception {
        // Given
        var factory = new SaslTermination();
        var filterFactoryContext = mock(FilterFactoryContext.class);

        @SuppressWarnings("unchecked")
        var credentialStoreService = mock(ScramCredentialStoreService.class);
        var credentialStore = mock(ScramCredentialStore.class);

        var mechanismConfig = new MechanismConfig(
                "TestCredentialStore",
                Map.of("test", "config"));

        var config = new SaslTerminationConfig(
                Map.of("SCRAM-SHA-256", mechanismConfig));

        when(filterFactoryContext.pluginInstance(
                eq(ScramCredentialStoreService.class),
                eq("TestCredentialStore")))
                .thenReturn(credentialStoreService);

        when(credentialStoreService.buildCredentialStore())
                .thenReturn(credentialStore);

        var context = factory.initialize(filterFactoryContext, config);

        // When
        factory.close(context);

        // Then
        verify(credentialStoreService).close();
    }

    @Test
    void shouldSuppressExceptionsWhenClosingService() throws Exception {
        // Given
        var factory = new SaslTermination();
        var filterFactoryContext = mock(FilterFactoryContext.class);

        @SuppressWarnings("unchecked")
        var credentialStoreService = mock(ScramCredentialStoreService.class);
        var credentialStore = mock(ScramCredentialStore.class);

        var mechanismConfig = new MechanismConfig(
                "TestCredentialStore",
                Map.of("test", "config"));

        var config = new SaslTerminationConfig(
                Map.of("SCRAM-SHA-256", mechanismConfig));

        when(filterFactoryContext.pluginInstance(
                eq(ScramCredentialStoreService.class),
                eq("TestCredentialStore")))
                .thenReturn(credentialStoreService);

        when(credentialStoreService.buildCredentialStore())
                .thenReturn(credentialStore);

        RuntimeException exception = new RuntimeException("Service failed to close");

        doThrow(exception).when(credentialStoreService).close();

        var context = factory.initialize(filterFactoryContext, config);

        // When
        RuntimeException thrown = null;
        try {
            factory.close(context);
        }
        catch (RuntimeException e) {
            thrown = e;
        }

        // Then
        assertThat(thrown).isNotNull();
        assertThat(thrown).isSameAs(exception);
    }
}
