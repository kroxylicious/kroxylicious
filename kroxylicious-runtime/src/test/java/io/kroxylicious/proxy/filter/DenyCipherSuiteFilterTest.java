/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class DenyCipherSuiteFilterTest {

    @Test
   void emptyCiphersDefaultsToDefaultCiphers() {
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3"));
        List<String> defaultAllowedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3");

        String[] filtered = DenyCipherSuiteFilter.INSTANCE.deniedCiphers(null)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertArrayEquals(defaultAllowedCiphers.toArray(), filtered);
    }

    @Test
    public void emptyCiphersDefaultsToDefaultCiphersMinusDenied() {
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3"));
        List<String> deniedCiphers = Arrays.asList("CIPHER_SUITE_2");
        List<String> defaultAllowedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_3");

        String[] filtered = DenyCipherSuiteFilter.INSTANCE.deniedCiphers(deniedCiphers)
                .filterCipherSuites(null, defaultCiphers, supportedCiphers);
        assertArrayEquals(defaultAllowedCiphers.toArray(), filtered);
    }

    @Test
    public void specificCiphersInstanceImplementsWithRequestedCiphers() {
        List<String> requestedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2");
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4"));

        String[] filtered = DenyCipherSuiteFilter.INSTANCE.deniedCiphers(null)
                .filterCipherSuites(requestedCiphers, defaultCiphers, supportedCiphers);
        assertArrayEquals(requestedCiphers.toArray(), filtered);
    }

    @Test
    public void specificCiphersInstanceImplementsWithRequestedCiphersMinusUnsupported() {
        List<String> requestedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_8");
        List<String> requestedSupportedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2");
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4"));

        String[] filtered = DenyCipherSuiteFilter.INSTANCE.deniedCiphers(null)
                .filterCipherSuites(requestedCiphers, defaultCiphers, supportedCiphers);
        assertArrayEquals(requestedSupportedCiphers.toArray(), filtered);
    }

    @Test
    public void specificCiphersInstanceImplementsWithRequestedCiphersMinusDenied() {
        List<String> requestedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3");
        List<String> deniedCiphers = Arrays.asList("CIPHER_SUITE_2");
        List<String> requestedAllowedCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_3");
        List<String> defaultCiphers = Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4");
        Set<String> supportedCiphers = new HashSet<>(Arrays.asList("CIPHER_SUITE_1", "CIPHER_SUITE_2", "CIPHER_SUITE_3", "CIPHER_SUITE_4"));

        String[] filtered = DenyCipherSuiteFilter.INSTANCE.deniedCiphers(deniedCiphers)
                .filterCipherSuites(requestedCiphers, defaultCiphers, supportedCiphers);
        assertArrayEquals(requestedAllowedCiphers.toArray(), filtered);
    }

}
