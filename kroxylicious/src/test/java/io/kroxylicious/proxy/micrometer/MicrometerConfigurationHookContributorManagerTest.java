/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MicrometerConfigurationHookContributorManagerTest {

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
    void testCommonTagsConfigType() {
        assertThat(getConfigType("CommonTags")).isEqualTo(CommonTagsHook.CommonTagsHookConfig.class);
    }

    @Test
    void testCommonTagsInstance() {
        assertThat(getInstance("CommonTags", new CommonTagsHook.CommonTagsHookConfig(Map.of()))).isInstanceOf(CommonTagsHook.class);
    }

    @Test
    void testStandardBindersConfigType() {
        assertThat(getConfigType("StandardBinders")).isEqualTo(StandardBindersHook.StandardBindersHookConfig.class);
    }

    @Test
    void testStandardBindersInstance() {
        assertThat(getInstance("StandardBinders", new StandardBindersHook.StandardBindersHookConfig(List.of()))).isInstanceOf(StandardBindersHook.class);
    }

    private MicrometerConfigurationHook getInstance(String shortName, BaseConfig config) {
        return MicrometerConfigurationHookContributorManager.getInstance().getInstance(shortName, config);
    }

    private static Class<? extends BaseConfig> getConfigType(String shortName) {
        return MicrometerConfigurationHookContributorManager.getInstance().getConfigType(shortName);
    }

}