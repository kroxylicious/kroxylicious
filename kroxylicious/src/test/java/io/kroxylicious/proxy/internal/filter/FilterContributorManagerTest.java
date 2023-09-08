/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.internal.filter.ProduceRequestTransformationFilter.UpperCasing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterContributorManagerTest {

    @Test
    void testNonExistentConfigType() {
        assertThatThrownBy(() -> getConfigType("nonexist")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNonExistentGetInstance() {
        BaseConfig config = new BaseConfig();
        assertThatThrownBy(() -> getInstance("nonexist", config)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testProduceRequestTransformationConfigType() {
        assertThat(getConfigType("ProduceRequestTransformation")).isEqualTo(ProduceRequestTransformationFilter.ProduceRequestTransformationConfig.class);
    }

    @Test
    void testFetchResponseTransformationConfigType() {
        assertThat(getConfigType("FetchResponseTransformation")).isEqualTo(FetchResponseTransformationFilter.FetchResponseTransformationConfig.class);
    }

    @Test
    void testProduceRequestTransformationInstance() {
        assertThat(getInstance("ProduceRequestTransformation", new ProduceRequestTransformationFilter.ProduceRequestTransformationConfig(
                UpperCasing.class.getName()))).isInstanceOf(
                        ProduceRequestTransformationFilter.class);
    }

    @Test
    void testFetchResponseTransformationInstance() {
        assertThat(getInstance("FetchResponseTransformation", new FetchResponseTransformationFilter.FetchResponseTransformationConfig(
                UpperCasing.class.getName()))).isInstanceOf(
                        FetchResponseTransformationFilter.class);
    }

    private Filter getInstance(String shortName, BaseConfig config) {
        return FilterContributorManager.getInstance().getInstance(shortName, config);
    }

    private static Class<? extends BaseConfig> getConfigType(String shortName) {
        return FilterContributorManager.getInstance().getConfigType(shortName);
    }

}