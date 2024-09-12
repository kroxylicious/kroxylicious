/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.net;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * An {@link InetAddressResolverProvider} that resolves all names ending {@code int.kroxylicious.test} to localhost.
 * All other names are resolved by the platform's built in resolver.
 * <p>
 * This exists in order to test functionality that depends on SNI without requiring real DNS names and/or modifications
 * to platform {@code /etc/hosts} etc.
 */
public class IntegrationTestInetAddressResolverProvider extends InetAddressResolverProvider {
    // Note there is no deliberately no logger in this class.
    // Logging frameworks have a habit of trying to resolve hostnames as part of initialisation
    // and thus break if they are initialised by this class.
    public static final String TEST_DOMAIN = ".int.kroxylicious.test";

    public IntegrationTestInetAddressResolverProvider() {

    }

    @Override
    public InetAddressResolver get(Configuration configuration) {
        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
                if (host != null && host.toLowerCase(Locale.ROOT).endsWith(TEST_DOMAIN)) {
                    var inetAddressStream = configuration.builtinResolver().lookupByName("localhost", lookupPolicy);
                    return inetAddressStream.map(in -> {
                        try {
                            return InetAddress.getByAddress(host, in.getAddress());
                        }
                        catch (UnknownHostException e) {
                            // Should never happen
                            throw new RuntimeException(e);
                        }
                    }).toList().stream();
                } else {
                    return configuration.builtinResolver().lookupByName(host, lookupPolicy);
                }
            }

            @Override
            public String lookupByAddress(byte[] addr) throws UnknownHostException {
                return configuration.builtinResolver().lookupByAddress(addr);
            }
        };
    }

    @Override
    public String name() {
        return IntegrationTestInetAddressResolverProvider.class.getSimpleName();
    }

    /**
     * Given an address prefix such as {@code myhost} or {@code myhost.sub}, generates a full qualified domain
     * name that is guaranteed to resolve to localhost.
     *
     * @param prefix address prefix, without dot suffix.
     * @return fully qualified host name.
     */
    public static String generateFullyQualifiedDomainName(String prefix) {
        Objects.requireNonNull(prefix, "prefix must not be null");
        if (prefix.endsWith(".")) {
            prefix = prefix.substring(0, prefix.lastIndexOf("."));
        }
        return prefix.concat(TEST_DOMAIN);

    }

    public static String generateFullyQualifiedDomainName(String prefix, int port) {
        return generateFullyQualifiedDomainName(prefix).concat(":").concat(Integer.toString(port));
    }
}
