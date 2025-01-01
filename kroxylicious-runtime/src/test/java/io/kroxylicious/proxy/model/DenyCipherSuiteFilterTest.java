/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DenyCipherSuiteFilterTest {

    @Test
    void emptyCiphersDefaultsToDefaultCiphers() {
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3"));
        List<String> defaultAllowedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3");

        String[] filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(defaultAllowedCiphers);
    }

    @Test
    void emptyCiphersDefaultsToDefaultCiphersMinusDenied() {
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3"));
        Set<String> deniedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_2", "CIPHER_SUITE_4"));
        List<String> defaultAllowedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_3");

        String[] filtered = new DenyCipherSuiteFilter(deniedCiphers)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(defaultAllowedCiphers);
    }

    @Test
    void specificCiphersInstanceImplementsWithRequestedCiphers() {
        List<String> requestedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2");
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4"));

        String[] filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(requestedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(requestedCiphers);
    }

    @Test
    void specificCiphersInstanceImplementsWithRequestedCiphersMinusUnsupported() {
        List<String> requestedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_8");
        List<String> requestedSupportedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2");
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4"));

        String[] filtered = new DenyCipherSuiteFilter(null)
                .filterCipherSuites(requestedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(requestedSupportedCiphers);
    }

    @Test
    void specificCiphersInstanceImplementsWithRequestedCiphersMinusDenied() {
        List<String> requestedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3");
        Set<String> deniedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_2"));
        List<String> requestedAllowedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_3");
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4"));

        String[] filtered = new DenyCipherSuiteFilter(deniedCiphers)
                .filterCipherSuites(requestedCiphers, defaultCiphers, supportedCiphers);
        assertThat(filtered).containsExactlyElementsOf(requestedAllowedCiphers);
    }

}
