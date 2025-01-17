/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Streams;

import static org.assertj.core.api.Assertions.assertThat;

class DenyCipherSuiteFilterTest {

    private final List<String> defaultCiphers = List.of("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");

    // Test platform supports the default ciphers plus a deprecated one.
    private final Set<String> supportedCiphers = Streams.concat(defaultCiphers.stream(), Stream.of("DEPRECATED_CIPHER_SUITE_1")).collect(Collectors.toSet());

    @Test
    void defaultsToDefaultCiphers() {
        var filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(defaultCiphers);
    }

    @Test
    void emptyCiphersGivesDefault() {
        var filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(List.of(), defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(defaultCiphers);
    }

    @Test
    void denyCiphers() {
        var deniedCiphers = Set.of("CIPHER_SUITE_2", "CIPHER_SUITE_4");
        var expectedAllowedCiphers = List.of("CIPHER_SUITE_1", "CIPHER_SUITE_3");

        var filtered = new DenyCipherSuiteFilter(deniedCiphers)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(expectedAllowedCiphers);
    }

    @Test
    void allowCiphers() {
        var allowedCiphers = List.of("CIPHER_SUITE_1", "CIPHER_SUITE_2");

        var filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(allowedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(allowedCiphers);
    }

    @Test
    void allowAndDenyCiphers() {
        var allowedCiphers = List.of("CIPHER_SUITE_3", "CIPHER_SUITE_1", "CIPHER_SUITE_2");
        var deniedCiphers = Set.of("CIPHER_SUITE_1");
        var expectedAllowedCiphers = List.of("CIPHER_SUITE_3", "CIPHER_SUITE_2");

        var filtered = new DenyCipherSuiteFilter(deniedCiphers)
                .filterCipherSuites(allowedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(expectedAllowedCiphers);
    }

    @Test
    void allowRespectsReordering() {
        var allowedCiphers = List.of("CIPHER_SUITE_3", "CIPHER_SUITE_1", "CIPHER_SUITE_2");

        var filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(allowedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(allowedCiphers);
    }

    @Test
    void allowsSupportedCipherToBeEnabled() {
        var allowedCiphers = List.of("DEPRECATED_CIPHER_SUITE_1", "CIPHER_SUITE_1");

        var filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(allowedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(allowedCiphers);
    }

    @Test
    void ignoresUnrecognizedAllowCipher() {
        var allowedCiphers = List.of("CIPHER_SUITE_1", "CIPHER_SUITE_2", "UNKNOWN_CIPHER");
        var expectedAllowedCiphers = List.of("CIPHER_SUITE_1", "CIPHER_SUITE_2");

        var filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(allowedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(expectedAllowedCiphers);
    }

    @Test
    void ignoresUnrecognizedDenyCipher() {
        var deniedCipher = Set.of("CIPHER_SUITE_2", "CIPHER_SUITE_3", "UNKNOWN_CIPHER");
        var expectedAllowedCiphers = List.of("CIPHER_SUITE_1", "CIPHER_SUITE_4");

        var filtered = new DenyCipherSuiteFilter(deniedCipher)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(expectedAllowedCiphers);
    }

}
