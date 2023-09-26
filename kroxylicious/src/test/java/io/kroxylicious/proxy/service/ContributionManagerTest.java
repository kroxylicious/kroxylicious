/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContributionManagerTest {

    private List<Contributor> contributingContributors;

    @BeforeEach
    void setUp() {
        contributingContributors = List.of(new StringContributor("v1"),
                new LongContributor(3), new IntContributor(), new InnerClassContributor("inner"));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldLoadServicesOfType() {
        // Given
        final Function<Class, Iterable> supplier = mock(Function.class);
        when(supplier.apply(StringContributor.class)).thenReturn(List.of(new StringContributor("testValue")));
        final ContributionManager contributionManager = new ContributionManager(supplier);

        // When
        contributionManager.getDefinition(StringContributor.class, StringContributor.class.getName());

        // Then
        verify(supplier).apply(any());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldLoadServicesOfTopLevelClassByShortName() {
        // Given
        ContributionManager manager = new ContributionManager(clazz -> contributingContributors);

        // When
        ContributionManager.ConfigurationDefinition definition = manager.getDefinition(IntContributor.class, "IntContributor");

        // Then
        assertThat(definition.configurationType()).isEqualTo(Void.class);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldNotLoadServicesOfStaticInnerClassByShortName() {
        // Given
        ContributionManager manager = new ContributionManager(clazz -> contributingContributors);

        // When
        assertThrows(IllegalArgumentException.class, () -> manager.getDefinition(StringContributor.class, "StringContributor"));

        // Then
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldNotLoadServicesOfNonStaticInnerClassByShortName() {
        // Given
        ContributionManager manager = new ContributionManager(clazz -> contributingContributors);

        // When
        assertThrows(IllegalArgumentException.class, () -> manager.getDefinition(StringContributor.class, "InnerClassContributor"));

        // Then
    }

    @Test
    void shouldLoadServicesOfMultipleType() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);
        final ContributionManager.ConfigurationDefinition stringConfigDef = contributionManager.getDefinition(StringContributor.class, StringContributor.class.getName());

        // When
        final ContributionManager.ConfigurationDefinition longConfigDef = contributionManager.getDefinition(LongContributor.class, LongContributor.class.getName());

        // Then
        assertThat(stringConfigDef).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(StringConfig.class);
        assertThat(longConfigDef).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(LongConfig.class);
    }

    @Test
    void shouldThrowExceptionIfShortNameIsUnknownForConfigurationDefinition() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        assertThrows(IllegalArgumentException.class, () -> contributionManager.getDefinition(StringContributor.class, "unknown"));

        // Then
    }

    @Test
    void shouldFindConfigDefinitionByClassName() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        final ContributionManager.ConfigurationDefinition configurationDefinition = contributionManager.getDefinition(StringContributor.class,
                StringContributor.class.getName());

        // Then
        assertThat(configurationDefinition).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(StringConfig.class);
    }

    @Test
    void shouldThrowExceptionIfNameIsUnknownForInstance() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        assertThrows(IllegalArgumentException.class, () -> contributionManager.createInstance(StringContributor.class, "unknown", () -> null));

        // Then
    }

    @Test
    void shouldCreateInstanceByClassName() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        final String actualInstance = contributionManager.createInstance(StringContributor.class, StringContributor.class.getName(), () -> null);

        // Then
        assertThat(actualInstance).isEqualTo("v1");
    }

    private static class LongContributor implements Contributor<Long, LongConfig, Context<LongConfig>> {
        private final long value;

        private LongContributor(long value) {
            this.value = value;
        }

        @NonNull
        @Override
        public Class<LongConfig> getConfigType() {
            return LongConfig.class;
        }

        @Override
        public Long createInstance(Context<LongConfig> context) {
            return value;
        }
    }

    private static class StringContributor implements Contributor<String, StringConfig, Context<StringConfig>> {
        private final String value;
        private final Class<StringConfig> configurationType;

        private StringContributor(String value) {
            this.value = value;
            this.configurationType = StringConfig.class;
        }

        @NonNull
        @Override
        public Class<StringConfig> getConfigType() {
            return configurationType;
        }

        @Override
        public String createInstance(Context<StringConfig> context) {
            return value;
        }
    }

    private class InnerClassContributor implements Contributor<String, StringConfig, Context<StringConfig>> {
        private final String value;
        private final Class<StringConfig> configurationType;

        private InnerClassContributor(String value) {
            this.value = value;
            this.configurationType = StringConfig.class;
        }

        @NonNull
        @Override
        public Class<StringConfig> getConfigType() {
            return configurationType;
        }

        @Override
        public String createInstance(Context<StringConfig> context) {
            return value;
        }
    }

    private static class StringConfig {

    }

    private static class LongConfig {

    }
}
