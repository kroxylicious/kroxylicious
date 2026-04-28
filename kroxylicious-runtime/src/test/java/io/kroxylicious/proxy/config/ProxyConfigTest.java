/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.OnVirtualClusterTerminalFailure.ServePolicy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ProxyConfigTest {

    @Test
    void defaultsShouldMatchExpectedPolicy() {
        var config = new ProxyConfig(null, null);
        assertThat(config.onVirtualClusterTerminalFailure()).isNotNull();
        assertThat(config.onVirtualClusterTerminalFailure().serve()).isEqualTo(ServePolicy.NONE);
        assertThat(config.configurationReload()).isNotNull();
        assertThat(config.configurationReload().onFailure().rollback()).isTrue();
        assertThat(config.configurationReload().persistToDisk()).isTrue();
    }

    @Test
    void preservesExplicitValues() {
        var terminalFailure = new OnVirtualClusterTerminalFailure(ServePolicy.SUCCESSFUL);
        var reload = new ConfigurationReload(
                new ConfigurationReload.OnFailure(false),
                false);
        var config = new ProxyConfig(terminalFailure, reload);
        assertThat(config.onVirtualClusterTerminalFailure().serve()).isEqualTo(ServePolicy.SUCCESSFUL);
        assertThat(config.configurationReload().onFailure().rollback()).isFalse();
        assertThat(config.configurationReload().persistToDisk()).isFalse();
    }

    @Test
    void staticDefaultIsEquivalentToAllNull() {
        assertThat(ProxyConfig.DEFAULT).isEqualTo(new ProxyConfig(null, null));
    }

    @Test
    void configurationReloadDefaultFieldsResolve() {
        var reload = new ConfigurationReload(null, null);
        assertThat(reload.onFailure().rollback()).isTrue();
        assertThat(reload.persistToDisk()).isTrue();
    }

    @Test
    void onFailureDefaultResolves() {
        assertThat(new ConfigurationReload.OnFailure(null).rollback()).isTrue();
    }

    @Test
    void shouldConstructWithoutError() {
        assertThatCode(() -> new ProxyConfig(null, null)).doesNotThrowAnyException();
        assertThatCode(() -> new OnVirtualClusterTerminalFailure(null)).doesNotThrowAnyException();
        assertThatCode(() -> new ConfigurationReload(null, null)).doesNotThrowAnyException();
    }
}
