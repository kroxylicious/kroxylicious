/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.FilterExecutors;

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
        BaseContributor.BaseContributorBuilder<Long, ContributorContext> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long, ContributorContext> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> one = baseContributor.getConfigType("one");
        assertThat(one).isEqualTo(BaseConfig.class);
    }

    @Test
    void testSupplier() {
        BaseContributor.BaseContributorBuilder<Long, ContributorContext> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long, ContributorContext> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("one", new BaseConfig(), getContext());
        assertThat(instance).isEqualTo(1L);
    }

    @Test
    void testSpecifyingConfigType() {
        BaseContributor.BaseContributorBuilder<Long, ContributorContext> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, ContributorContext> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> configType = baseContributor.getConfigType("fromBaseConfig");
        assertThat(configType).isEqualTo(LongConfig.class);
    }

    @Test
    void testSpecifyingConfigTypeInstance() {
        BaseContributor.BaseContributorBuilder<Long, ContributorContext> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, ContributorContext> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("fromBaseConfig", new LongConfig(), getContext());
        assertThat(instance).isEqualTo(2L);
    }

    @Test
    void testAccessingContext() {
        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        BaseContributor.BaseContributorBuilder<Long, FilterContributorContext> builder = BaseContributor.builder();
        AtomicReference<ScheduledExecutorService> received = new AtomicReference<>();
        builder.add("fromBaseConfig", LongConfig.class, (filterContributorContext, longConfig) -> {
            received.set(filterContributorContext.executors().filterExecutor());
            return longConfig.value;
        });
        BaseContributor<Long, FilterContributorContext> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("fromBaseConfig", new LongConfig(), new FilterContributorContext() {
            @Override
            public FilterExecutors executors() {
                return () -> scheduledExecutorService;
            }
        });
        assertThat(instance).isEqualTo(2L);
        assertThat(received.get()).isEqualTo(scheduledExecutorService);
    }

    @Test
    void testFailsIfConfigNotAssignableToSpecifiedType() {
        BaseContributor.BaseContributorBuilder<Long, ContributorContext> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, ContributorContext> baseContributor = new BaseContributor<>(builder) {
        };
        AnotherConfig incompatibleConfig = new AnotherConfig();
        assertThatThrownBy(() -> {
            baseContributor.getInstance("fromBaseConfig", incompatibleConfig, getContext());
        }).isInstanceOf(IllegalArgumentException.class);
    }

    private static ContributorContext getContext() {
        return new ContributorContext() {

        };
    }
}
