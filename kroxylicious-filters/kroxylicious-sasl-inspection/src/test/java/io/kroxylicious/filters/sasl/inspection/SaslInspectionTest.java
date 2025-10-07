/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaslInspectionTest {

    @Mock
    FilterFactoryContext filterFactoryContext;

    @Mock
    SaslObserverFactory insecure;
    @Mock
    SaslObserverFactory secure;

    @Test
    void shouldDefaultToDisablingObserversThatUseCleartextPasswords() {
        // Given
        var factory = new SaslInspection();

        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Secure", "Insecure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Secure")).thenReturn(secure);
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Insecure")).thenReturn(insecure);

        when(secure.mechanismName()).thenReturn("SECURE_MECH");
        when(insecure.transmitsCredentialInCleartext()).thenReturn(true);

        factory.initialize(filterFactoryContext, null);

        // When
        var map = factory.getObserverFactoryMap();

        // Then
        assertThat(map).containsKeys("SECURE_MECH");
    }

    @Test
    void shouldBeAbleToEnableObserversThatUseCleartextPasswords() {
        // Given
        var factory = new SaslInspection();

        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Insecure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Insecure")).thenReturn(insecure);

        when(insecure.mechanismName()).thenReturn("INSECURE_MECH");
        when(insecure.transmitsCredentialInCleartext()).thenReturn(true);

        factory.initialize(filterFactoryContext, new Config(Set.of("INSECURE_MECH")));

        // When
        var map = factory.getObserverFactoryMap();

        // Then
        assertThat(map).containsKeys("INSECURE_MECH");
    }

    @Test
    void shouldDetectObserversRegisteringCollidingSaslMechNames() {
        // Given
        var factory = new SaslInspection();

        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("OBS1", "OBS2"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "OBS1")).thenReturn(new StubSaslObserverFactory1("MECH_NAME"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "OBS2")).thenReturn(new StubSaslObserverFactory2("MECH_NAME"));

        // When/Then
        assertThatThrownBy(() -> factory.initialize(filterFactoryContext, null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageMatching("StubSaslObserverFactory\\d and StubSaslObserverFactory\\d both register the same SASL mechanism name MECH_NAME");
    }

    @Test
    void shouldDetectUseOfUnrecognisedSaslMechNames() {
        // Given
        var factory = new SaslInspection();

        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Secure", "Insecure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Secure")).thenReturn(secure);
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Insecure")).thenReturn(insecure);

        when(secure.mechanismName()).thenReturn("SECURE_MECH");
        when(insecure.mechanismName()).thenReturn("INSECURE_MECH");

        var config = new Config(Set.of("SECURE_MECH", "UNKNOWN"));

        // When/Then
        assertThatThrownBy(() -> factory.initialize(filterFactoryContext, config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessage("The following enabled SASL mechanism names are unknown: [UNKNOWN]");
    }

    @Test
    void shouldRejectNoObservers() {
        // Given
        var factory = new SaslInspection();

        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Insecure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Insecure")).thenReturn(insecure);
        when(insecure.transmitsCredentialInCleartext()).thenReturn(true);

        // When/Then
        assertThatThrownBy(() -> factory.initialize(filterFactoryContext, null))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("The SaslObserver requires at at least one enabled SaslObserver implementation");
    }

    private record StubSaslObserverFactory1(String mechanismName) implements SaslObserverFactory {

        @Override
        public SaslObserver createObserver() {
            throw new UnsupportedOperationException();
        }
    }

    private record StubSaslObserverFactory2(String mechanismName) implements SaslObserverFactory {

        @Override
        public SaslObserver createObserver() {
            throw new UnsupportedOperationException();
        }
    }
}