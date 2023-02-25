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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTenantSniInetAddressResolverProvider extends InetAddressResolverProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiTenantSniInetAddressResolverProvider.class);

    public MultiTenantSniInetAddressResolverProvider() {
        LOGGER.info("Creating MultiTenantSniInetAddressResolverProvider");
    }

    @Override
    public InetAddressResolver get(Configuration configuration) {
        return new InetAddressResolver() {
            @Override
            public Stream<InetAddress> lookupByName(String host, LookupPolicy lookupPolicy) throws UnknownHostException {
                if (host != null && host.endsWith(".multitenant.kafka")) {
                    Stream<InetAddress> inetAddressStream = configuration.builtinResolver().lookupByName("localhost", lookupPolicy);
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
        return MultiTenantSniInetAddressResolverProvider.class.getSimpleName();
    }
}
