/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.proxy.config.NettySettings;
import io.kroxylicious.proxy.config.NetworkDefinition;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NetworkDefinitionBuilderTest {

    @Test
    void shouldReturnNullWhenSpecIsNull() {
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .build();
        // @formatter:on
        assertThat(NetworkDefinitionBuilder.build(proxy)).isNull();
    }

    @Test
    void shouldReturnNullWhenNetworkIsNull() {
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec().endSpec()
                .build();
        // @formatter:on
        assertThat(NetworkDefinitionBuilder.build(proxy)).isNull();
    }

    @Test
    void shouldBuildNetworkDefinitionWithProxySettingsOnly() {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewProxy()
                            .withWorkerThreadCount(4)
                            .withShutdownQuietPeriod("2s")
                            .withAuthenticatedIdleTimeout("10m")
                            .withUnauthenticatedIdleTimeout("30s")
                        .endProxy()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        var expectedProxySettings = new NettySettings(
                Optional.of(4),
                Optional.of(Duration.ofSeconds(2)),
                Optional.empty(),
                Optional.of(Duration.ofMinutes(10)),
                Optional.of(Duration.ofSeconds(30)));

        // when / then
        assertThat(NetworkDefinitionBuilder.build(proxy))
                .isEqualTo(new NetworkDefinition(null, expectedProxySettings));
    }

    @Test
    void shouldBuildNetworkDefinitionWithManagementSettingsOnly() {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewManagement()
                            .withWorkerThreadCount(2)
                            .withShutdownQuietPeriod("5s")
                        .endManagement()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        var expectedMgmtSettings = new NettySettings(
                Optional.of(2),
                Optional.of(Duration.ofSeconds(5)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // when / then
        assertThat(NetworkDefinitionBuilder.build(proxy))
                .isEqualTo(new NetworkDefinition(expectedMgmtSettings, null));
    }

    @Test
    void shouldBuildNetworkDefinitionWithBothManagementAndProxySettings() {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewProxy()
                            .withWorkerThreadCount(4)
                            .withShutdownQuietPeriod("2s")
                            .withAuthenticatedIdleTimeout("10m")
                            .withUnauthenticatedIdleTimeout("30s")
                        .endProxy()
                        .withNewManagement()
                            .withWorkerThreadCount(2)
                            .withShutdownQuietPeriod("5s")
                        .endManagement()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        var expectedProxySettings = new NettySettings(
                Optional.of(4),
                Optional.of(Duration.ofSeconds(2)),
                Optional.empty(),
                Optional.of(Duration.ofMinutes(10)),
                Optional.of(Duration.ofSeconds(30)));
        var expectedMgmtSettings = new NettySettings(
                Optional.of(2),
                Optional.of(Duration.ofSeconds(5)),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        // when / then
        assertThat(NetworkDefinitionBuilder.build(proxy))
                .isEqualTo(new NetworkDefinition(expectedMgmtSettings, expectedProxySettings));
    }

    @Test
    void shouldReturnEmptyOptionalsWhenNettySettingsFieldsAreAbsent() {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewProxy().endProxy()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        // when / then
        assertThat(NetworkDefinitionBuilder.build(proxy))
                .isEqualTo(new NetworkDefinition(null, new NettySettings(
                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())));
    }

    @ParameterizedTest
    @CsvSource({
            "500ms, PT0.5S",
            "2s,    PT2S",
            "90s,   PT1M30S",
            "1m,    PT1M",
            "1h,    PT1H",
            "1h30m, PT1H30M"
    })
    void shouldParseShutdownQuietPeriodToDuration(String durationStr, String expectedIso) {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewProxy()
                            .withShutdownQuietPeriod(durationStr)
                        .endProxy()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        // when
        NetworkDefinition result = NetworkDefinitionBuilder.build(proxy);

        // then
        assertThat(result).isNotNull();
        assertThat(result.proxy()).isNotNull();
        assertThat(result.proxy().shutdownQuietPeriod())
                .isEqualTo(Optional.of(Duration.parse(expectedIso)));
    }

    @Test
    void shouldParseShutdownTimeoutToDuration() {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewProxy()
                            .withShutdownTimeout("30s")
                        .endProxy()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        // when
        NetworkDefinition result = NetworkDefinitionBuilder.build(proxy);

        // then
        assertThat(result).isNotNull();
        assertThat(result.proxy()).isNotNull();
        assertThat(result.proxy().shutdownTimeout())
                .isEqualTo(Optional.of(Duration.ofSeconds(30)));
    }

    @Test
    void shouldThrowIllegalStateExceptionForInvalidDuration() {
        // given
        // @formatter:off
        var proxy = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName("test")
                .endMetadata()
                .withNewSpec()
                    .withNewNetwork()
                        .withNewProxy()
                            .withShutdownQuietPeriod("not-a-duration")
                        .endProxy()
                    .endNetwork()
                .endSpec()
                .build();
        // @formatter:on

        // when / then
        assertThatThrownBy(() -> NetworkDefinitionBuilder.build(proxy))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("not-a-duration");
    }
}
