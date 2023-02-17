/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.filter.KrpcFilterContext;

public interface AddressMapping {
    String downstreamHost(KrpcFilterContext context, String upstreamHost, int upstreamPort);

    int downstreamPort(KrpcFilterContext context, String upstreamHost, int upstreamPort);
}
