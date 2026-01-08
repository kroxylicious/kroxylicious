/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

import nl.altindag.log.LogCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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

    static List<Arguments> shouldLogWarningOnInstantiation() {
        return List.of(
                Arguments.argumentSet("@Deprecated",
                        "io.kroxylicious.proxy.config.DeprecatedImplementation",
                        "io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                                + "with name 'io.kroxylicious.proxy.config.DeprecatedImplementation' is deprecated."),
                Arguments.argumentSet("shouldWarnAboutRenamedPluginLoadedWithOldFqName",
                        "io.kroxylicious.proxy.config.ImplementationWithDeprecatedName",
                        "io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                                + "with name 'io.kroxylicious.proxy.config.ImplementationWithDeprecatedName' "
                                + "should now be referred to using the name 'io.kroxylicious.proxy.config.RenamedImplementation'. "
                                + "The plugin has been renamed and in the future the old name "
                                + "'io.kroxylicious.proxy.config.ImplementationWithDeprecatedName' will cease to work."),
                Arguments.argumentSet("shouldWarnAboutRenamedPluginLoadedWithOldSimpleName",
                        "ImplementationWithDeprecatedName",
                        "io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                                + "with name 'ImplementationWithDeprecatedName' "
                                + "should now be referred to using the name 'io.kroxylicious.proxy.config.RenamedImplementation'. "
                                + "The plugin has been renamed and in the future the old name "
                                + "'ImplementationWithDeprecatedName' will cease to work."),
                Arguments.argumentSet("shouldWarnAboutRepackagedPluginLoadedWithOldFqName",
                        "io.kroxylicious.proxy.config.oldpkg.RepackagedImplementation",
                        "io.kroxylicious.proxy.config.ServiceWithBaggage plugin "
                                + "with name 'io.kroxylicious.proxy.config.oldpkg.RepackagedImplementation' "
                                + "should now be referred to using the name 'io.kroxylicious.proxy.config.newpkg.RepackagedImplementation'. "
                                + "The plugin has been renamed and in the future the old name "
                                + "'io.kroxylicious.proxy.config.oldpkg.RepackagedImplementation' will cease to work."));
    }

    @ParameterizedTest
    @MethodSource
    void shouldLogWarningOnInstantiation(String instanceName, String expectedMessage) {
        // Given
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithBaggage.class);
        try (LogCaptor logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class)) {
            // When
            var instance = factory.pluginInstance(instanceName);
            // Then
            assertThat(instance).isNotNull();
            assertThat(logCaptor.hasWarnMessage(expectedMessage)).isTrue();
        }
    }

    static List<Arguments> shouldNotLogWarningOnInstantiation() {
        return List.of(
                Arguments.argumentSet("for RenamedPluginLoadedWithNewFqName",
                        "io.kroxylicious.proxy.config.RenamedImplementation"),
                Arguments.argumentSet("for RenamedPluginLoadedWithNewSimpleName",
                        "RenamedImplementation"),
                Arguments.argumentSet("for RepackagedPluginLoadedWithNewFqName",
                        "io.kroxylicious.proxy.config.newpkg.RepackagedImplementation"),
                Arguments.argumentSet("shouldNotWarnAboutRepackagedPluginLoadedWithSimpleName",
                        "RepackagedImplementation"));
    }

    @ParameterizedTest
    @MethodSource
    void shouldNotLogWarningOnInstantiation(String instanceName) {
        // Given
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithBaggage.class);
        try (LogCaptor logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class)) {
            // When
            var instance = factory.pluginInstance("RepackagedImplementation");
            // Then
            assertThat(instance).isNotNull();
            assertThat(logCaptor.getWarnLogs()).isEmpty();
        }
    }

    @Test
    void shouldNotReturnPluginInstanceForAmbiguousName() {
        PluginFactory<ServiceWithAmbiguousImpls> factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithAmbiguousImpls.class);
        String ambiguous1 = io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getSimpleName();
        String ambiguous2 = io.kroxylicious.proxy.config.ambiguous2.Ambiguous.class.getSimpleName();

        assertThrows(UnknownPluginInstanceException.class, () -> factory.pluginInstance(ambiguous1));
        assertThrows(UnknownPluginInstanceException.class, () -> factory.pluginInstance(ambiguous2));
    }

    @Test
    void shouldNotReturnPluginInstanceForAmbiguousNameWithDeprecatedName() {
        ServiceBasedPluginFactoryRegistry serviceBasedPluginFactoryRegistry = new ServiceBasedPluginFactoryRegistry();
        try (LogCaptor logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class)) {
            assertThatThrownBy(() -> serviceBasedPluginFactoryRegistry.pluginFactory(ServiceWithCollidingAlias.class))
                    .isExactlyInstanceOf(RuntimeException.class)
                    .hasMessage("Ambiguous plugin implementation name 'io.kroxylicious.proxy.config.ServiceWithCollidingAliasX'");
            assertThat(logCaptor.hasWarnMessage("Plugin implementation class io.kroxylicious.proxy.config.ServiceWithCollidingAliasY "
                    + "is annotated with @DeprecatedPluginName(oldName=\"io.kroxylicious.proxy.config.ServiceWithCollidingAliasX\") "
                    + "which collides with the real plugin implementation class io.kroxylicious.proxy.config.ServiceWithCollidingAliasX. "
                    + "You must remove one of these classes from the class path."))
                    .isTrue();
        }
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
