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
import java.util.stream.Stream;

/**
 * An {@link InetAddressResolverProvider} that resolves all subdomains of localhost to localhost.
 * <p>
 * The lowkey mock supports behaving like Azure, handling domains like ${myvaultname}.localhost.
 * Not all platforms can resolve these local hosts, so we use the JDK 18+ resolver provider to
 * intercept DNS resolution and resolve them to localhost.
 */
public class LocalhostSubdomainResolverProvider extends InetAddressResolverProvider {
    // supports Azure lowkey mock which uses ${vaultName}.localhost hostnames, which some platforms cannot resolve
    public static final String LOCALHOST_SUBDOMAIN = ".localhost";

    @Override
    public InetAddressResolver get(Configuration configuration) {
        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
                if (host != null && host.toLowerCase(Locale.ROOT).endsWith(LOCALHOST_SUBDOMAIN)) {
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
                }
                else {
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
        return LocalhostSubdomainResolverProvider.class.getSimpleName();
    }

}
