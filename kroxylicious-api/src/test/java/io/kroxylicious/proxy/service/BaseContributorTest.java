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
    void shouldReturnTrueForConfigurationRequiredViaDefaulting() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", LongConfig.class, longConfig -> 1L);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        boolean actual = baseContributor.getConfigDefinition("one").configurationRequired();

        assertThat(actual).isTrue();
    }

    @Test
    void shouldReturnTrueForExplicitlySpecifiedConfigurationRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", LongConfig.class, (longConfig, context) -> 1L, true);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        boolean actual = baseContributor.getConfigDefinition("one").configurationRequired();

        assertThat(actual).isTrue();
    }

    @Test
    void shouldReturnFalseFroOptionalRegisteredConfig() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", LongConfig.class, (longConfig, context) -> 1L, false);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        boolean actual = baseContributor.getConfigDefinition("one").configurationRequired();

        assertThat(actual).isFalse();
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
    void testDefaultConfigClassFromConfigDefinition() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        ConfigurationDefinition one = baseContributor.getConfigDefinition("one");
        assertThat(one).hasFieldOrPropertyWithValue("configurationType", BaseConfig.class);
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
    void testSpecifyingConfigTypeViaConfigDefinition() {
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };
        ConfigurationDefinition actualConfigDefinition = baseContributor.getConfigDefinition("fromBaseConfig");
        assertThat(actualConfigDefinition).hasFieldOrPropertyWithValue("configurationType", LongConfig.class);
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
        }, true);
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

    @Test
    void shouldFailIfConfigIsNullAndRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("requiresConfig", LongConfig.class, (baseConfig, context) -> 1L, true);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        assertThatThrownBy(() -> baseContributor.getInstance("requiresConfig", () -> null)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'requiresConfig' requires configuration but none was supplied");

        // Then
    }

    @Test
    void shouldConstructInstanceIfConfigIsNullAndNotRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("noConfigRequired", LongConfig.class, (longConfig, context) -> 1L, false);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("noConfigRequired", () -> null);

        // Then
        assertThat(actualInstance).isNotNull();
    }

    @Test
    void shouldConstructInstanceIfConfigTypeIsSpecifiedButNotRequiredAndConfigIsNull() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("configOptional", LongConfig.class, (longConfig, context) -> 1L, false);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("configOptional", () -> null);

        // Then
        assertThat(actualInstance).isNotNull();
    }

    @Test
    void shouldConstructInstanceIfConfigIsNotNullAndRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("configRequired", LongConfig.class, longConfig -> longConfig.value);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("configRequired", LongConfig::new);

        // Then
        assertThat(actualInstance).isNotNull();
    }

    @Test
    void shouldConstructInstanceIfConfigIsNotNullAndNotRequired() {
        // Given
        BaseContributor.BaseContributorBuilder<Long, Context> builder = BaseContributor.builder();
        builder.add("configOptional", () -> 1L);
        BaseContributor<Long, Context> baseContributor = new BaseContributor<>(builder) {
        };

        // When
        final Long actualInstance = baseContributor.getInstance("configOptional", () -> null);

        // Then
        assertThat(actualInstance).isNotNull();
    }
}