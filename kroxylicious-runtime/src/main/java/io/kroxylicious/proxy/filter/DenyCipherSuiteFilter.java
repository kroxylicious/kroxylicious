/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import io.netty.handler.ssl.CipherSuiteFilter;
import io.netty.util.internal.EmptyArrays;

/**
 * This class has been built to filter cipher suites based on requirements for Kroxylicious
 * If no ciphers are declared then the default platform ciphers will be used
 * If ciphers are declared then they are used instead of the default platform ciphers
 * Both lists would then have anything removed that wasn't supported or in the deniedCiphers
 */
public final class DenyCipherSuiteFilter implements CipherSuiteFilter {

    public static final DenyCipherSuiteFilter INSTANCE = new DenyCipherSuiteFilter();

    private DenyCipherSuiteFilter() {
    }

    private List<String> deniedCiphers;

    public DenyCipherSuiteFilter deniedCiphers(List<String> deniedCiphers) {
        this.deniedCiphers = deniedCiphers == null ? new ArrayList<>() : deniedCiphers;
        return this;
    }

    @Override
    public String[] filterCipherSuites(Iterable<String> ciphers, List<String> defaultCiphers,
                                       Set<String> supportedCiphers) {
        return ciphers == null ? allowedCiphers(defaultCiphers.size(), defaultCiphers, supportedCiphers)
                : allowedCiphers(supportedCiphers.size(), ciphers, supportedCiphers);
    }

    private String[] allowedCiphers(int newListSize, Iterable<String> ciphersToUse, Set<String> supportedCiphers) {
        List<String> newCiphers = new ArrayList<>(newListSize);
        for (String cipher : ciphersToUse) {
            if (cipher == null) {
                break;
            }
            if (supportedCiphers.contains(cipher) && !deniedCiphers.contains(cipher)) {
                newCiphers.add(cipher);
            }
        }

        return newCiphers.toArray(EmptyArrays.EMPTY_STRINGS);
    }
}
