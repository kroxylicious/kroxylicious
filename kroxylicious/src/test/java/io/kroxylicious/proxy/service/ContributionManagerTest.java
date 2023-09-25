/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.List;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
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
        contributingContributors = List.of(new StringContributor("one", "v1"), new StringContributor("two", "v2"),
                new LongContributor("three", 3));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    void shouldLoadServicesOfType() {
        // Given
        final Function<Class, Iterable> supplier = mock(Function.class);
        when(supplier.apply(StringContributor.class)).thenReturn(List.of(new StringContributor("testType", "testValue")));
        final ContributionManager contributionManager = new ContributionManager(supplier);

        // When
        contributionManager.getDefinition(StringContributor.class, "testType");

        // Then
        verify(supplier).apply(any());
    }

    @Test
    void shouldLoadServicesOfMultipleType() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);
        final ContributionManager.ConfigurationDefinition stringConfigDef = contributionManager.getDefinition(StringContributor.class, "two");

        // When
        final ContributionManager.ConfigurationDefinition longConfigDef = contributionManager.getDefinition(LongContributor.class, "three");

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
    void shouldFindConfigDefinitionByShortName() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        final ContributionManager.ConfigurationDefinition configurationDefinition = contributionManager.getDefinition(StringContributor.class, "two");

        // Then
        assertThat(configurationDefinition).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(StringConfig.class);
    }

    @Test
    void shouldThrowExceptionIfShortNameIsUnknownForInstance() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        assertThrows(IllegalArgumentException.class, () -> contributionManager.getInstance(StringContributor.class, "unknown", () -> null));

        // Then
    }

    @Test
    void shouldGetInstanceByShortName() {
        // Given
        final ContributionManager contributionManager = new ContributionManager(clazz -> contributingContributors);

        // When
        final String actualInstance = contributionManager.getInstance(StringContributor.class, "two", () -> null);

        // Then
        assertThat(actualInstance).isEqualTo("v2");
    }

    private static class LongContributor implements Contributor<Long, LongConfig, Context<LongConfig>> {
        private final String myTypeName;
        private final long value;

        private LongContributor(String typeName, long value) {
            this.myTypeName = typeName;
            this.value = value;
        }

        @NotNull
        @Override
        public String getTypeName() {
            return myTypeName;
        }

        @NonNull
        @Override
        public Class<LongConfig> getConfigType() {
            return LongConfig.class;
        }

        @Override
        public Long getInstance(Context<LongConfig> context) {
            return value;
        }
    }

    private static class StringContributor implements Contributor<String, StringConfig, Context<StringConfig>> {

        private final String myTypeName;
        private final String value;
        private final Class<StringConfig> configurationType;

        private StringContributor(String typeName, String value) {
            this.myTypeName = typeName;
            this.value = value;
            this.configurationType = StringConfig.class;
        }

        @NotNull
        @Override
        public String getTypeName() {
            return myTypeName;
        }

        @NonNull
        @Override
        public Class<StringConfig> getConfigType() {
            return configurationType;
        }

        @Override
        public String getInstance(Context<StringConfig> context) {
            return value;
        }
    }

    private static class StringConfig {

    }

    private static class LongConfig {

    }
}
