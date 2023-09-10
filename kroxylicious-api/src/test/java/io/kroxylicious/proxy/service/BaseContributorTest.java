/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        Optional<SpecificContributor<Long>> factory = baseContributor.getSpecificContributor("one");
        assertThat(factory).isPresent();
        assertThat(factory.get().getConfigClass()).isEqualTo(BaseConfig.class);
    }

    @Test
    void testSupplier() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Optional<SpecificContributor<Long>> factory = baseContributor.getSpecificContributor("one");
        assertThat(factory).isPresent();
        Long instance = factory.get().getInstance(new BaseConfig());
        assertThat(instance).isEqualTo(1L);
    }

    @Test
    void testSpecifyingConfigType() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Optional<SpecificContributor<Long>> factory = baseContributor.getSpecificContributor("fromBaseConfig");
        assertThat(factory).isPresent();
        assertThat(factory.get().getConfigClass()).isEqualTo(LongConfig.class);
    }

    @Test
    void testSpecifyingConfigTypeInstance() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };

        Optional<SpecificContributor<Long>> factory = baseContributor.getSpecificContributor("fromBaseConfig");
        assertThat(factory).isPresent();
        assertThat(factory.get().getInstance(new LongConfig())).isEqualTo(2L);
    }

    @Test
    void testFailsIfConfigNotAssignableToSpecifiedType() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        AnotherConfig incompatibleConfig = new AnotherConfig();
        Optional<SpecificContributor<Long>> factory = baseContributor.getSpecificContributor("fromBaseConfig");
        assertThat(factory).isPresent();
        SpecificContributor<Long> specificContributor = factory.get();
        assertThatThrownBy(() -> {
            specificContributor.getInstance(incompatibleConfig);
        }).isInstanceOf(IllegalArgumentException.class);
    }

}
