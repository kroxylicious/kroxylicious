/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;

import static io.kroxylicious.proxy.service.Context.wrap;
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
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> one = baseContributor.getConfigType("one");
        assertThat(one).isEqualTo(BaseConfig.class);
    }

    @Test
    void testSupplier() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("one", wrap(new BaseConfig()));
        assertThat(instance).isEqualTo(1L);
    }

    @Test
    void testNullConfigIsAllowed() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("one", wrap(null));
        assertThat(instance).isEqualTo(1L);
    }

    @Test
    void testContextAndConfigFunction() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        AtomicReference<Context> contextRef = new AtomicReference<>();
        builder.add("one", (context -> {
            contextRef.set(context);
            return 1L;
        }));
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Context context = wrap(new BaseConfig());
        Long instance = baseContributor.getInstance("one", context);
        assertThat(instance).isEqualTo(1L);
        assertThat(contextRef).hasValue(context);
    }

    @Test
    void testSpecifyingConfigType() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> configType = baseContributor.getConfigType("fromBaseConfig");
        assertThat(configType).isEqualTo(LongConfig.class);
    }

    @Test
    void testSpecifyingConfigTypeInstance() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("fromBaseConfig", wrap(new LongConfig()));
        assertThat(instance).isEqualTo(2L);
    }

    @Test
    void testSpecifyingConfigTypeInstanceAndContext() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        AtomicReference<Context> contextRef = new AtomicReference<>();
        builder.add("fromBaseConfig", LongConfig.class, (context, baseConfig) -> {
            contextRef.set(context);
            return baseConfig.value;
        });
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        Context context = wrap(new LongConfig());
        Long instance = baseContributor.getInstance("fromBaseConfig", context);
        assertThat(instance).isEqualTo(2L);
        assertThat(contextRef).hasValue(context);
    }

    @Test
    void testFailsIfConfigNotAssignableToSpecifiedType() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        final Context wrappedContext = wrap(new AnotherConfig());

        assertThatThrownBy(() -> baseContributor.getInstance("fromBaseConfig", wrappedContext)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldGetConfigDefinitionForKnownTypeName() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        ConfigurationDefinition actual = baseContributor.getConfigDefinition("fromBaseConfig");

        // Then
        assertThat(actual).isNotNull().hasFieldOrPropertyWithValue("configurationType", LongConfig.class);
    }

    @Test
    void shouldThrowForUnknownTypeNameConfigDefinition() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        assertThatThrownBy(() -> baseContributor.getConfigDefinition("fromBaseConfig")).isInstanceOf(IllegalArgumentException.class);

        // Then
    }
}