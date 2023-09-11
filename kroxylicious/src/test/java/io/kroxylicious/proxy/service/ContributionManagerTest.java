/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.List;
import java.util.function.Function;

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

    @SuppressWarnings("unchecked")
    @Test
    void shouldLoadServicesOfType() {
        //Given
        final Function<Class<StringContributor>, Iterable<StringContributor>> supplier = mock(Function.class);
        final ContributionManager<StringContributor> contributionManager = new ContributionManager<>(supplier);
        when(supplier.apply(StringContributor.class)).thenReturn(List.of(new StringContributor(true, "testType")));

        //When
            contributionManager.getDefinition(StringContributor.class, "testType");

        //Then
        verify(supplier).apply(any());
    }

    @Test
    void shouldThrowExceptionIfShortNameIsUnknown() {
        //Given
        List<StringContributor> contributors = List.of(new StringContributor(false, "one"), new StringContributor(false, "two"));
        final ContributionManager<StringContributor> contributionManager = new ContributionManager<>(clazz -> contributors);

        //When
        assertThrows(IllegalArgumentException.class, () -> contributionManager.getDefinition(StringContributor.class, "unknown"));

        //Then
    }

    @Test
    void shouldFindConfigTypeByShortName() {
        //Given
        List<StringContributor> contributors = List.of(new StringContributor(false, "one"), new StringContributor(true, "two", StringyConfig.class));
        final ContributionManager<StringContributor> contributionManager = new ContributionManager<>(clazz -> contributors);

        //When
        final ConfigurationDefinition configurationDefinition = contributionManager.getDefinition(StringContributor.class, "two");

        //Then
        assertThat(configurationDefinition).hasFieldOrProperty("configurationType").extracting("configurationType").isEqualTo(StringyConfig.class);
    }

    private class StringContributor implements Contributor<String, Context> {

        private final boolean contributes;
        private final String value;
        private final Class<? extends BaseConfig> configurationType;

        private StringContributor(boolean contributes, String value) {
            this(contributes, value, StringConfig.class);
        }

        private StringContributor(boolean contributes, String value, Class<? extends BaseConfig> configurationType) {
            this.contributes = contributes;
            this.value = value;
            this.configurationType = configurationType;
        }

        @Override
        public boolean contributes(String shortName) {
            return contributes;
        }

        @Override
        public Class<? extends BaseConfig> getConfigType(String shortName) {
            return getConfigDefinition(shortName).configurationType();
        }

        @Override
        public ConfigurationDefinition getConfigDefinition(String shortName) {
            return new ConfigurationDefinition(configurationType);
        }

        @Override
        public String getInstance(String shortName, Context context) {
            return value;
        }
    }

    private static class StringConfig extends BaseConfig {

    }

    private static class StringyConfig extends BaseConfig {

    }
}