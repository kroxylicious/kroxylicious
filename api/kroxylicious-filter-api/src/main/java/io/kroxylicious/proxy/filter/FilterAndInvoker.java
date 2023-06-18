/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * A Filter and it's respective invoker
 * @param filter filter
 * @param invoker invoker
 */
public record FilterAndInvoker(KrpcFilter filter, FilterInvoker invoker) {

    /**
     * Builds an invoker for a filter
     * @param filter filter
     * @return a filter and it's respective invoker
     */
    public static FilterAndInvoker build(KrpcFilter filter) {
        return new FilterAndInvoker(filter, FilterInvokers.from(filter));
    }
}
