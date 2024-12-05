/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.Configuration;

import static io.kroxylicious.proxy.ProxyEnvironment.DEVELOPMENT;
import static io.kroxylicious.proxy.ProxyEnvironment.PRODUCTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProxyEnvironmentTest {

    static List<Arguments> permittedInternalConfigurationForEnv() {
        List<Arguments> cases = new ArrayList<>();
        cases.add(Arguments.of(DEVELOPMENT, null));
        cases.add(Arguments.of(DEVELOPMENT, Map.of()));
        cases.add(Arguments.of(DEVELOPMENT, Map.of("a", "b")));
        cases.add(Arguments.of(PRODUCTION, null));
        cases.add(Arguments.of(PRODUCTION, Map.of()));
        return cases;
    }

    @ParameterizedTest
    @MethodSource
    void permittedInternalConfigurationForEnv(ProxyEnvironment env, Map<String, Object> internalConfiguration) {
        Configuration configuration = new Configuration(null, null, List.of(), null, false, Optional.ofNullable(internalConfiguration));
        assertThat(env.validate(configuration)).isEqualTo(configuration);
    }

    @Test
    void nonEmptyInternalDisallowedForProduction() {
        Configuration configuration = new Configuration(null, null, List.of(), null, false, Optional.of(Map.of("a", "b")));
        assertThatThrownBy(() -> {
            PRODUCTION.validate(configuration);
        }).isInstanceOf(EnvironmentConfigurationException.class).hasMessage("internal configuration for proxy present in production environment");
    }

}
