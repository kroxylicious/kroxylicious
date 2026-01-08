/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import nl.altindag.log.LogCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServiceBasedPluginFactoryRegistryTest {

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldThrowIfNullPluginType() {
        ServiceBasedPluginFactoryRegistry reg = new ServiceBasedPluginFactoryRegistry();
        assertThrows(NullPointerException.class, () -> {
            reg.pluginFactory(null);
        });
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldThrowIfNullPluginInstance() {
        ServiceBasedPluginFactoryRegistry reg = new ServiceBasedPluginFactoryRegistry();
        var f = reg.pluginFactory(NotAService.class);
        assertThrows(NullPointerException.class, () -> {
            f.pluginInstance(null);
        });
    }

    @Test
    void shouldThrowIfEmptyPluginInstance() {
        ServiceBasedPluginFactoryRegistry reg = new ServiceBasedPluginFactoryRegistry();
        var f = reg.pluginFactory(NotAService.class);
        assertThrows(IllegalArgumentException.class, () -> {
            f.pluginInstance("");
        });
    }

    @Test
    void shouldThrowIfUnknownPluginInstance() {
        ServiceBasedPluginFactoryRegistry reg = new ServiceBasedPluginFactoryRegistry();
        var f = reg.pluginFactory(NotAService.class);
        assertThrows(UnknownPluginInstanceException.class, () -> {
            f.pluginInstance("unheardof");
        });
    }

    @Test
    void shouldReturnPluginInstanceForFQButAmbiguousName() {
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithAmbiguousImpls.class);
        assertInstanceOf(io.kroxylicious.proxy.config.ambiguous1.Ambiguous.class,
                factory.pluginInstance(io.kroxylicious.proxy.config.ambiguous1.Ambiguous.class.getName()));
        assertInstanceOf(io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class,
                factory.pluginInstance(io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getName()));
    }

    @Test
    void shouldWarnAboutDeprecatedPluginImpl() {
        // Given
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithBaggage.class);
        LogCaptor logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class);
        // When
        var instance = factory.pluginInstance("io.kroxylicious.proxy.config.DeprecatedImplementation");
        // Then
        assertThat(instance).isNotNull();
        assertThat(logCaptor.hasWarnMessage("io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                + "with name 'io.kroxylicious.proxy.config.DeprecatedImplementation' is deprecated."))
                .isTrue();
    }

    @Test
    void shouldWarnAboutPluginLoadedWithOldFqName() {
        // Given
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithBaggage.class);
        LogCaptor logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class);
        // When
        var instance = factory.pluginInstance("io.kroxylicious.proxy.config.ImplementationWithDeprecatedName");
        // Then
        assertThat(instance).isNotNull();
        assertThat(logCaptor.hasWarnMessage("io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                + "with name 'io.kroxylicious.proxy.config.ImplementationWithDeprecatedName' "
                + "should now be referred to using the name 'io.kroxylicious.proxy.config.RenamedImplementation'. "
                + "The plugin has been renamed since 0.0.0 and in the future the old name "
                + "'io.kroxylicious.proxy.config.ImplementationWithDeprecatedName' will cease to work."))
                .isTrue();
    }

    @Test
    void shouldWarnAboutPluginLoadedWithOldShortName() {
        // Given
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithBaggage.class);
        LogCaptor logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class);
        // When
        var instance = factory.pluginInstance("ImplementationWithDeprecatedName");
        // Then
        assertThat(instance).isNotNull();
        assertThat(logCaptor.hasWarnMessage("io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                + "with name 'ImplementationWithDeprecatedName' "
                + "should now be referred to using the name 'io.kroxylicious.proxy.config.RenamedImplementation'. "
                + "The plugin has been renamed since 0.0.0 and in the future the old name "
                + "'ImplementationWithDeprecatedName' will cease to work."))
                .isTrue();
    }

    @Test
    void shouldNotReturnPluginInstanceForAmbiguousName() {
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithAmbiguousImpls.class);
        String ambiguous1 = io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getSimpleName();
        String ambiguous2 = io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getSimpleName();

        assertThrows(UnknownPluginInstanceException.class, () -> factory.pluginInstance(ambiguous1));
        assertThrows(UnknownPluginInstanceException.class, () -> factory.pluginInstance(ambiguous2));
    }

    @Test
    void shouldKnowConfigType() {
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithAmbiguousImpls.class);
        assertEquals(Void.class,
                factory.configType(io.kroxylicious.proxy.config.ambiguous1.Ambiguous.class.getName()));

        assertEquals(String.class,
                factory.configType(io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getName()));
    }

    @Test
    void shouldReportRegisteredNames() {
        var names = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithAmbiguousImpls.class).registeredInstanceNames();
        assertThat(names).containsExactly("io.kroxylicious.proxy.config.ambiguous1.Ambiguous", "io.kroxylicious.proxy.config.ambiguous2.Ambiguous");
    }
}
