/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.resourceisolation;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.filter.resourceisolation.PrincipalResourceNameMapperService.Config;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.User;

import static org.assertj.core.api.Assertions.assertThat;

class PrincipalResourceNameMapperServiceTest {

    private static Stream<Arguments> configs() {
        return Stream.of(Arguments.argumentSet("null config", null, User.class),
                Arguments.argumentSet("null principal type", new Config(null), User.class),
                Arguments.argumentSet("explicit principal type", new Config(CustomPrincipal.class), CustomPrincipal.class));
    }

    @ParameterizedTest
    @MethodSource(value = "configs")
    void acceptsConfig(Config config, Class<? extends Principal> expectedPrincipalType) {
        // Given
        var service = new PrincipalResourceNameMapperService();

        // When
        service.initialize(config);

        // Then
        assertThat(service.getEffectiveConfig())
                .isNotNull()
                .extracting(Config::principalType)
                .isEqualTo(expectedPrincipalType);
    }

    @Test
    void shouldBuildMapper() {
        // Given
        var service = new PrincipalResourceNameMapperService();
        service.initialize(null);

        // When
        var mapper = service.build();

        // Then
        assertThat(mapper).isNotNull();
    }

    static class CustomPrincipal implements Principal {
        @Override
        public String name() {
            throw new UnsupportedOperationException();
        }
    }

}