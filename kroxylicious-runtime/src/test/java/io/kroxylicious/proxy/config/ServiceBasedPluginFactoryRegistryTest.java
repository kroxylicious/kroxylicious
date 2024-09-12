/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ServiceBasedPluginFactoryRegistryTest {

    @Test
    void shouldThrowIfNullPluginType() {
        ServiceBasedPluginFactoryRegistry reg = new ServiceBasedPluginFactoryRegistry();
        assertThrows(NullPointerException.class, () -> {
            reg.pluginFactory(null);
        });
    }

    @Test
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
        assertInstanceOf(
                io.kroxylicious.proxy.config.ambiguous1.Ambiguous.class,
                factory.pluginInstance(io.kroxylicious.proxy.config.ambiguous1.Ambiguous.class.getName())
        );
        assertInstanceOf(
                io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class,
                factory.pluginInstance(io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getName())
        );
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
        assertEquals(
                Void.class,
                factory.configType(io.kroxylicious.proxy.config.ambiguous1.Ambiguous.class.getName())
        );

        assertEquals(
                String.class,
                factory.configType(io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getName())
        );
    }
}
