/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.micrometer;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.micrometer.TestMicrometerConfigurationHookContributor.Config;
import io.kroxylicious.proxy.service.ContributorContext;

import static io.kroxylicious.proxy.micrometer.TestMicrometerConfigurationHookContributor.SHORT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MicrometerConfigurationHookContributorManagerTest {

    public static final MicrometerConfigurationHookContributorManager INSTANCE = MicrometerConfigurationHookContributorManager.getInstance();

    @Test
    void testGetConfigType() {
        Class<? extends BaseConfig> clazz = INSTANCE.getConfigType(SHORT_NAME);
        assertThat(clazz).isEqualTo(Config.class);
    }

    @Test
    void testGetConfigTypeWhenNoContributorMatchesShortName() {
        assertThatThrownBy(() -> INSTANCE.getConfigType("mismatched"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("No micrometer configuration hook found for name 'mismatched'");
    }

    @Test
    void testGetHook() {
        Config config = new Config();
        MicrometerConfigurationHook hook = INSTANCE.getHook(SHORT_NAME, config);
        assertThat(hook).isInstanceOf(TestHook.class);
        TestHook hook1 = (TestHook) hook;
        assertThat(hook1.shortName()).isEqualTo(SHORT_NAME);
        assertThat(hook1.context()).isEqualTo(ContributorContext.instance());
        assertThat(hook1.config()).isSameAs(config);
    }

    @Test
    void testGetHookTypeWhenNoContributorMatchesShortName() {
        Config config = new Config();
        assertThatThrownBy(() -> INSTANCE.getHook("mismatched", config))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("No micrometer configuration hook found for name 'mismatched'");
    }

}