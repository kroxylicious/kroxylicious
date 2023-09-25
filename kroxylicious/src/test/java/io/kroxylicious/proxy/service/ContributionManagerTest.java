/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.BaseConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContributionManagerTest {

    private List<Contributor<?, ? super Context>> contributingContributors;

    @BeforeEach
    void setUp() {
        contributingContributors = List.of(new StringContributor("one", "v1"), new StringContributor("two", "v2", StringyConfig.class),
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
        final ConfigurationDefinition stringConfigDef = contributionManager.getDefinition(StringContributor.class, "two");

        // When
        final ConfigurationDefinition longConfigDef = contributionManager.getDefinition(LongContributor.class, "three");

        // Then
        assertThat(stringConfigDef).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(StringyConfig.class);
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
        final ConfigurationDefinition configurationDefinition = contributionManager.getDefinition(StringContributor.class, "two");

        // Then
        assertThat(configurationDefinition).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(StringyConfig.class);
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

    private static class LongContributor implements Contributor<Long, Context> {
        private final String myTypeName;
        private final long value;

        private LongContributor(String typeName, long value) {
            this.myTypeName = typeName;
            this.value = value;
        }

        @Override
        public boolean contributes(String typeName) {
            return Objects.equals(this.myTypeName, typeName);
        }

        @Override
        public Class<? extends BaseConfig> getConfigType(String shortName) {
            return LongConfig.class;
        }

        @Override
        public ConfigurationDefinition getConfigDefinition(String shortName) {
            return new ConfigurationDefinition(LongConfig.class, true);
        }

        @Override
        public Long getInstance(String shortName, Context context) {
            return value;
        }
    }

    private static class StringContributor implements Contributor<String, Context> {

        private final String myTypeName;
        private final String value;
        private final Class<? extends BaseConfig> configurationType;

        private StringContributor(String typeName, String value) {
            this(typeName, value, StringConfig.class);
        }

        private StringContributor(String typeName, String value, Class<? extends BaseConfig> configurationType) {
            this.myTypeName = typeName;
            this.value = value;
            this.configurationType = configurationType;
        }

        @Override
        public boolean contributes(String typeName) {
            return Objects.equals(this.myTypeName, typeName);
        }

        @Override
        public Class<? extends BaseConfig> getConfigType(String shortName) {
            return getConfigDefinition(shortName).configurationType();
        }

        @Override
        public ConfigurationDefinition getConfigDefinition(String shortName) {
            return new ConfigurationDefinition(configurationType, true);
        }

        @Override
        public String getInstance(String shortName, Context context) {
            return value;
        }
    }

    private static class StringConfig extends BaseConfig {

    }

    private static class LongConfig extends BaseConfig {

    }

    private static class StringyConfig extends BaseConfig {

    }
}