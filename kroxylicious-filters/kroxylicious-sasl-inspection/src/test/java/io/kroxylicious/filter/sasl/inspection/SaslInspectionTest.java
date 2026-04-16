/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.sasl.inspection;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.authentication.UserFactory;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.internal.subject.DefaultSaslSubjectBuilderService;
import io.kroxylicious.proxy.internal.subject.Map;
import io.kroxylicious.proxy.internal.subject.PrincipalAdderConf;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.test.schema.SchemaValidationAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaslInspectionTest {

    @Mock
    FilterFactoryContext filterFactoryContext;

    @Mock
    ResolvedPluginRegistry resolvedPluginRegistry;

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

        factory.initialize(filterFactoryContext, new Config(Set.of("INSECURE_MECH"), null, null, false));

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

        var config = new Config(Set.of("SECURE_MECH", "UNKNOWN"), null, null, false);

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

    @Test
    void shouldHaveLegacyAndConfig2PluginAnnotations() {
        Plugin[] annotations = SaslInspection.class.getAnnotationsByType(Plugin.class);

        assertThat(annotations).hasSize(2);

        var versionToConfigType = java.util.Arrays.stream(annotations)
                .collect(java.util.stream.Collectors.toMap(Plugin::configVersion, Plugin::configType));

        assertThat(versionToConfigType).containsOnly(
                entry("", Config.class),
                entry("v1alpha1", ConfigV1.class));
    }

    @Test
    void fullConfigShouldPassSchemaValidation() {
        // Config with all fields populated that Java accepts
        new ConfigV1(
                Set.of("PLAIN", "SCRAM-SHA-256"),
                "my-subject-builder",
                true);

        // Same config in raw YAML form
        SchemaValidationAssert.assertSchemaAccepts("SaslInspection", "v1alpha1", java.util.Map.of(
                "enabledMechanisms", List.of("PLAIN", "SCRAM-SHA-256"),
                "subjectBuilder", "my-subject-builder",
                "requireAuthentication", true));
    }

    @Test
    void minimalConfigShouldPassSchemaValidation() {
        // Minimal config — all fields optional
        new ConfigV1(null, null, null);

        SchemaValidationAssert.assertSchemaAccepts("SaslInspection", "v1alpha1", Collections.emptyMap());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    void shouldInitializeWithV1Config() {
        // Given
        var factory = new SaslInspection();
        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Secure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Secure")).thenReturn(secure);
        when(secure.mechanismName()).thenReturn("SECURE_MECH");

        var v1Config = new ConfigV1(
                Set.of("SECURE_MECH"),
                "my-subject-builder",
                false);

        when(filterFactoryContext.resolvedPluginRegistry()).thenReturn(Optional.of(resolvedPluginRegistry));
        when(resolvedPluginRegistry.pluginInstance(SaslSubjectBuilderService.class, "my-subject-builder"))
                .thenReturn(new DefaultSaslSubjectBuilderService());
        when(resolvedPluginRegistry.pluginConfig(
                eq("io.kroxylicious.proxy.authentication.SaslSubjectBuilderService"), eq("my-subject-builder")))
                .thenReturn(new DefaultSaslSubjectBuilderService.Config(List.of(new PrincipalAdderConf("saslAuthorizedId",
                        List.of(new Map("/(.*)/$1/U", null)),
                        UserFactory.class.getName()))));

        // When/Then
        factory.initialize(filterFactoryContext, v1Config);
        Filter filter = factory.createFilter(filterFactoryContext, null);
        assertThat(filter).isNotNull();
    }

    @Test
    void shouldRejectV1ConfigWithoutRegistry() {
        // Given
        var factory = new SaslInspection();
        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Secure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Secure")).thenReturn(secure);
        when(secure.mechanismName()).thenReturn("SECURE_MECH");

        var v1Config = new ConfigV1(
                Set.of("SECURE_MECH"),
                "my-subject-builder",
                false);

        // When/Then
        assertThatThrownBy(() -> factory.initialize(filterFactoryContext, v1Config))
                .isInstanceOf(PluginConfigurationException.class)
                .hasMessageContaining("ResolvedPluginRegistry");
    }

    @Test
    void shouldBeAbleToCreateInspectorWithACustomSubjectBuilder() {
        // Given
        var factory = new SaslInspection();
        when(filterFactoryContext.pluginImplementationNames(SaslObserverFactory.class)).thenReturn(Set.of("Secure"));
        when(filterFactoryContext.pluginInstance(SaslObserverFactory.class, "Secure")).thenReturn(secure);

        when(secure.mechanismName()).thenReturn("SECURE_MECH");
        when(filterFactoryContext.pluginInstance(SaslSubjectBuilderService.class, DefaultSaslSubjectBuilderService.class.getName()))
                .thenReturn(new DefaultSaslSubjectBuilderService());

        // When
        factory.initialize(filterFactoryContext,
                new Config(Set.of("SECURE_MECH"),
                        DefaultSaslSubjectBuilderService.class.getName(),
                        new DefaultSaslSubjectBuilderService.Config(List.of(new PrincipalAdderConf("saslAuthorizedId",
                                List.of(new Map("/(.*)/$1/U", null)),
                                UserFactory.class.getName()))),
                        false));
        Filter filter = factory.createFilter(filterFactoryContext, null);

        // Then
        assertThat(filter).isNotNull();
    }
}
