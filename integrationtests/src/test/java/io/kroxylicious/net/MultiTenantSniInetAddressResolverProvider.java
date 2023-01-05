/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.net;

import io.kroxylicious.proxy.KafkaClusterIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.util.stream.Stream;

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
                    return configuration.builtinResolver().lookupByName(configuration.lookupLocalHostName(), lookupPolicy);
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
