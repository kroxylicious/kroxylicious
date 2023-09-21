/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.ConfigurationDefinition;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class ConfigurableFilterContributorTest {

    public static final String TYPE_NAME = "testName";

    @Test
    void testTypeName() {
        // given
        ConfigurableFilterContributor<TestConfig> contributor = createContributor(true);
        // when
        String typeName = contributor.getTypeName();
        // then
        assertThat(typeName).isEqualTo(TYPE_NAME);
    }

    @Test
    void testRequiredConfig() {
        // given
        ConfigurableFilterContributor<TestConfig> contributor = createContributor(true);
        // when
        ConfigurationDefinition configDefinition = contributor.getConfigDefinition();
        // then
        assertThat(configDefinition).isEqualTo(new ConfigurationDefinition(TestConfig.class, true));
    }

    @Test
    void testOptionalConfig() {
        // given
        ConfigurableFilterContributor<TestConfig> contributor = createContributor(false);
        // when
        ConfigurationDefinition configDefinition = contributor.getConfigDefinition();
        // then
        assertThat(configDefinition).isEqualTo(new ConfigurationDefinition(TestConfig.class, false));
    }

    @Test
    void testGetInstance() {
        // given
        ConfigurableFilterContributor<TestConfig> contributor = createContributor(false);
        FilterConstructContext mock = Mockito.mock(FilterConstructContext.class);
        TestConfig testConfig = new TestConfig();
        when(mock.getConfig()).thenReturn(testConfig);
        // when
        Filter instance = contributor.getInstance(mock);
        // then
        assertThat(instance).isInstanceOfSatisfying(TestFilter.class, testFilter -> {
            assertThat(testFilter.getConfig()).isSameAs(testConfig);
        });
    }

    @Test
    void testGetInstanceWhenConfigDoesntMatchConfigType() {
        // given
        ConfigurableFilterContributor<TestConfig> contributor = createContributor(false);
        FilterConstructContext mock = Mockito.mock(FilterConstructContext.class);
        when(mock.getConfig()).thenReturn(new BaseConfig());
        // when
        ThrowingCallable callable = () -> contributor.getInstance(mock);
        // then
        assertThatThrownBy(callable).isInstanceOf(IllegalStateException.class);
    }

    private ConfigurableFilterContributor<TestConfig> createContributor(final boolean configRequired) {
        return new ConfigurableFilterContributor<>(TYPE_NAME, TestConfig.class, configRequired) {

            @NonNull
            @Override
            protected Filter getInstance(FilterConstructContext context, TestConfig config) {
                return new TestFilter(config);
            }
        };
    }

    private static class TestConfig extends BaseConfig {
    }

    private static class TestFilter implements RequestFilter {
        private final TestConfig config;

        TestFilter(TestConfig config) {
            this.config = config;
        }

        @Override
        public CompletionStage<RequestFilterResult> onRequest(ApiKeys apiKey, RequestHeaderData header, ApiMessage request, FilterContext context) {
            return null;
        }

        public TestConfig getConfig() {
            return config;
        }
    }
}
