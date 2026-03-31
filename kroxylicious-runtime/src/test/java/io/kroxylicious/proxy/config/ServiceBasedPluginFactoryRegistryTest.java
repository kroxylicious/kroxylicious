/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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

    private static LogCaptor logCaptor;

    @BeforeAll
    static void setupLogCaptor() {
        logCaptor = LogCaptor.forClass(ServiceBasedPluginFactoryRegistry.class);
    }

    @AfterEach
    void clearLogs() {
        logCaptor.clearLogs();
    }

    @AfterAll
    static void tearDown() {
        logCaptor.close();
    }

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
                        "plugin is deprecated",
                        Map.of("pluginClass", "io.kroxylicious.proxy.config.ServiceWithBaggage",
                                "name", "io.kroxylicious.proxy.config.DeprecatedImplementation")),
                Arguments.argumentSet("shouldWarnAboutRenamedPluginLoadedWithOldFqName",
                        "io.kroxylicious.proxy.config.ImplementationWithDeprecatedName",
                        "plugin should now be referred to using the new name, the plugin has been renamed and in the future the old name will cease to work",
                        Map.of("pluginClass", "io.kroxylicious.proxy.config.ServiceWithBaggage",
                                "oldName", "io.kroxylicious.proxy.config.ImplementationWithDeprecatedName",
                                "newName", "io.kroxylicious.proxy.config.RenamedImplementation")),
                Arguments.argumentSet("shouldWarnAboutRenamedPluginLoadedWithOldSimpleName",
                        "ImplementationWithDeprecatedName",
                        "plugin should now be referred to using the new name, the plugin has been renamed and in the future the old name will cease to work",
                        Map.of("pluginClass", "io.kroxylicious.proxy.config.ServiceWithBaggage",
                                "oldName", "ImplementationWithDeprecatedName",
                                "newName", "io.kroxylicious.proxy.config.RenamedImplementation")),
                Arguments.argumentSet("shouldWarnAboutRepackagedPluginLoadedWithOldFqName",
                        "io.kroxylicious.proxy.config.oldpkg.RepackagedImplementation",
                        "plugin should now be referred to using the new name, the plugin has been renamed and in the future the old name will cease to work",
                        Map.of("pluginClass", "io.kroxylicious.proxy.config.ServiceWithBaggage",
                                "oldName", "io.kroxylicious.proxy.config.oldpkg.RepackagedImplementation",
                                "newName", "io.kroxylicious.proxy.config.newpkg.RepackagedImplementation")));
    }

    @ParameterizedTest
    @MethodSource
    void shouldLogWarningOnInstantiation(String instanceName, String expectedMessage, Map<String, String> expectedKeyValues) {
        // Given
        var factory = new ServiceBasedPluginFactoryRegistry().pluginFactory(ServiceWithBaggage.class);
        // When
        var instance = factory.pluginInstance(instanceName);
        // Then
        assertThat(instance).isNotNull();
        assertThat(logCaptor.getLogEvents()).singleElement()
                .satisfies(log -> {
                    assertThat(log.getMessage()).isEqualTo(expectedMessage);
                    var keyValuePairs = log.getKeyValuePairs();
                    expectedKeyValues.forEach((key, value) -> assertThat(keyValuePairs).contains(Map.entry(key, value)));
                });
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
        // When
        var instance = factory.pluginInstance("RepackagedImplementation");
        // Then
        assertThat(instance).isNotNull();
        assertThat(logCaptor.getWarnLogs()).isEmpty();
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
    void shouldThrowForAmbiguousNameWithDeprecatedName() {
        ServiceBasedPluginFactoryRegistry serviceBasedPluginFactoryRegistry = new ServiceBasedPluginFactoryRegistry();
        assertThatThrownBy(() -> serviceBasedPluginFactoryRegistry.pluginFactory(ServiceWithCollidingAlias.class))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("Ambiguous plugin implementation name 'io.kroxylicious.proxy.config.ServiceWithCollidingAliasX'");
        assertThat(logCaptor.getLogEvents()).singleElement()
                .satisfies(log -> {
                    assertThat(log.getMessage()).isEqualTo(
                            "plugin implementation class is annotated with @DeprecatedPluginName which collides with another plugin implementation class, you must remove one of these classes from the class path");
                    assertThat(log.getKeyValuePairs())
                            .contains(Map.entry("annotatedClass", "io.kroxylicious.proxy.config.ServiceWithCollidingAliasY"))
                            .contains(Map.entry("annotation", "DeprecatedPluginName"))
                            .contains(Map.entry("oldName", "io.kroxylicious.proxy.config.ServiceWithCollidingAliasX"))
                            .contains(Map.entry("collidingClass", "io.kroxylicious.proxy.config.ServiceWithCollidingAliasX"));
                });
    }

    @Test
    void shouldThrowForAmbiguousNameWithDeprecatedName2() {
        ServiceBasedPluginFactoryRegistry serviceBasedPluginFactoryRegistry = new ServiceBasedPluginFactoryRegistry();
        assertThatThrownBy(() -> serviceBasedPluginFactoryRegistry.pluginFactory(ServiceWithCollidingAlias2.class))
                .isExactlyInstanceOf(RuntimeException.class)
                .hasMessage("Ambiguous plugin implementation name 'io.kroxylicious.proxy.config.ServiceWithCollidingAlias2Z'");
        assertThat(logCaptor.getLogEvents()).singleElement()
                .satisfies(log -> {
                    assertThat(log.getMessage()).isEqualTo(
                            "plugin implementation class is annotated with @DeprecatedPluginName which collides with another plugin implementation class, you must remove one of these classes from the class path");
                    assertThat(log.getKeyValuePairs())
                            .contains(Map.entry("annotatedClass", "io.kroxylicious.proxy.config.ServiceWithCollidingAlias2X"))
                            .contains(Map.entry("annotation", "DeprecatedPluginName"))
                            .contains(Map.entry("oldName", "io.kroxylicious.proxy.config.ServiceWithCollidingAlias2Z"))
                            .contains(Map.entry("collidingClass", "io.kroxylicious.proxy.config.ServiceWithCollidingAlias2Y"));
                });
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
