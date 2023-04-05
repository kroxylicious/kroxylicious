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

class BaseContributorTest {
    static class LongConfig extends BaseConfig {
        private final Long value = 2L;

    }

    static class AnotherConfig extends BaseConfig {
    }

    static class LongClusterEndpointProvider implements ClusterEndpointProvider {
        @Override
        public String getClusterBootstrapAddress() {
            return "3";
        }

        @Override
        public String getBrokerAddress(int nodeId) {
            return "localhost:1234";
        }
    }

    @Test
    public void testDefaultConfigClass() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> one = baseContributor.getConfigType("one");
        assertThat(one).isEqualTo(BaseConfig.class);
    }

    @Test
    public void testSupplier() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("one", () -> 1L);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("one", new LongClusterEndpointProvider(), new BaseConfig());
        assertThat(instance).isEqualTo(1L);
    }

    @Test
    public void testSpecifyingConfigType() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Class<? extends BaseConfig> configType = baseContributor.getConfigType("fromBaseConfig");
        assertThat(configType).isEqualTo(LongConfig.class);
    }

    @Test
    public void testSpecifyingConfigTypeInstance() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("fromBaseConfig", new LongClusterEndpointProvider(), new LongConfig());
        assertThat(instance).isEqualTo(2L);
    }

    @Test
    public void testFailsIfConfigNotAssignableToSpecifiedType() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromBaseConfig", LongConfig.class, baseConfig -> baseConfig.value);
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        AnotherConfig incompatibleConfig = new AnotherConfig();
        assertThatThrownBy(() -> {
            baseContributor.getInstance("fromBaseConfig", new LongClusterEndpointProvider(), incompatibleConfig);
        }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testUsingProxyConfig() {
        BaseContributor.BaseContributorBuilder<Long> builder = BaseContributor.builder();
        builder.add("fromProxyConfig", LongConfig.class, (proxyConfig, longConfig) -> Long.valueOf(proxyConfig.getClusterBootstrapAddress()));
        BaseContributor<Long> baseContributor = new BaseContributor<>(builder) {
        };
        Long instance = baseContributor.getInstance("fromProxyConfig", new LongClusterEndpointProvider(), new LongConfig());
        assertThat(instance).isEqualTo(3L);
    }

}