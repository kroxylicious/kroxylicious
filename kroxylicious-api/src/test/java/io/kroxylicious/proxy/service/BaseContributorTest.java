/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BaseContributorTest {
    static class LongConfig extends BaseConfig {
        private final Long value = 2L;

    }

    static class AnotherConfig extends BaseConfig {
    }

    @Test
    void testDefaultConfigClass() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> one = baseContributor.getConfigType("one");
        assertThat(one).isEqualTo(BaseConfig.class);
    }

    @Test
    void testSupplier() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("one", new BaseConfig());
        assertThat(instance).isEqualTo(1L);
    }

    @Test
    void testSpecifyingConfigType() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> configType = baseContributor.getConfigType("fromBaseConfig");
        assertThat(configType).isEqualTo(LongConfig.class);
    }

    @Test
    void testSpecifyingConfigTypeInstance() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("fromBaseConfig", new LongConfig());
        assertThat(instance).isEqualTo(2L);
    }

    @Test
    void testFailsIfConfigNotAssignableToSpecifiedType() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        AnotherConfig incompatibleConfig = new AnotherConfig();
        assertThatThrownBy(() -> baseContributor.getInstance("fromBaseConfig", incompatibleConfig)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldFailIfConfigIsNullAndRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("requiresConfig", LongConfig.class, baseConfig -> 1L, true);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final IllegalArgumentException illegalArgumentException = assertThrows(IllegalArgumentException.class,
                () -> baseContributor.getInstance("requiresConfig", null));

        // Then
        assertThat(illegalArgumentException).hasMessageContaining("requiresConfig requires config but it is null");
    }

    @Test
    void shouldConstructInstanceIfConfigIsNullAndNotRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("noConfigRequired", LongConfig.class, longConfig -> 1L, false);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("noConfigRequired", null);

        // Then
        assertThat(actualInstance).isNotNull();
    }

    @Test
    void shouldConstructInstanceIfConfigTypeIsSpecifiedButNotRequiredAndConfigIsNull() {
        // Given
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("configOptional", LongConfig.class, longConfig -> 1L, false);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("configOptional", null);

        // Then
        assertThat(actualInstance).isNotNull();
    }

    @Test
    void shouldConstructInstanceIfConfigIsNotNullAndRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("configRequired", LongConfig.class, longConfig -> longConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("configRequired", new LongConfig());

        // Then
        assertThat(actualInstance).isNotNull();
    }

    @Test
    void shouldConstructInstanceIfConfigIsNotNullAndNotRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("configOptional", () -> 1L);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("configOptional", new LongConfig());

        // Then
        assertThat(actualInstance).isNotNull();
    }
}
