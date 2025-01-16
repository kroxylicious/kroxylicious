/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.CipherSuiteFilter;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * This class has been built to filter cipher suites based on requirements for Kroxylicious
 * If no ciphers are declared then the default platform ciphers will be used
 * If ciphers are declared then they are used instead of the default platform ciphers
 * Both lists would then have anything removed that wasn't supported or in the deniedCiphers
 */
public final class DenyCipherSuiteFilter implements CipherSuiteFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DenyCipherSuiteFilter.class);

    public DenyCipherSuiteFilter(@Nullable Set<String> deniedCiphers) {
        this.deniedCiphers = deniedCiphers == null ? new HashSet<>() : deniedCiphers;
    }

    private final Set<String> deniedCiphers;

    @Override
    public String[] filterCipherSuites(Iterable<String> ciphers, List<String> defaultCiphers,
                                       Set<String> supportedCiphers) {
        var actualCiphers = (ciphers == null || !ciphers.iterator().hasNext()) ? defaultCiphers : ciphers;

        StreamSupport.stream(actualCiphers.spliterator(), false)
                .filter(Predicate.not(supportedCiphers::contains))
                .forEach(unsupportedCipher -> LOGGER.warn("Ignoring allowed cipher '{}' as it is not recognized by this platform (supported ciphers: {})",
                        unsupportedCipher, supportedCiphers));

        deniedCiphers.stream()
                .filter(Predicate.not(supportedCiphers::contains))
                .forEach(unsupportedCipher -> LOGGER.warn("Ignoring denied cipher '{}' as it is not recognized by this platform (supported ciphers: {})",
                        unsupportedCipher, supportedCiphers));

        return StreamSupport.stream(actualCiphers.spliterator(), false)
                .filter(supportedCiphers::contains)
                .filter(Predicate.not(deniedCiphers::contains))
                .toList()
                .toArray(new String[]{});
    }
}
