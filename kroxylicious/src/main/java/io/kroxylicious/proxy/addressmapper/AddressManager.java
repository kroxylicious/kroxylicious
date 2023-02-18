/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.addressmapper;

import java.io.Closeable;

import io.kroxylicious.proxy.filter.NetFilterContext;

/**
 * AddressManagers understand how to map between upstream and downstream broker addresses.
 *
 * The AddressManager is used by the {@link io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler} to determine which backend to
 * connect to as connections are made to the upstream.
 *
 * It is also available for use by the filter implementations to map broker addresses returned from the downstream
 * into the addressing scheme required the upstream. {@link io.kroxylicious.proxy.internal.filter.BrokerAddressFilter}
 * is an example of a filter that does this.
 *
 * The AddressManager acts as the factory of the AddressMapper object.  The front end handler will obtain a
 * {@link AddressMapper} object for a connection before the upstream connection is made.  All filters in the chain will see
 * the same {@link AddressMapper}
 *
 * There is exactly one instance of the AddressManager. This is created on startup.  On shutdown  {@link #close()} will
 * be called.  Implementations may use close to release any outside resources they have acquired.
 *
 * AddressManager implementation are made available to the service loader via the {@link AddressManagerContributor}.
 */
public interface AddressManager extends Closeable {
    AddressMapper createMapper(NetFilterContext netFilterContext);

    @Override
    default void close() {
    }
}