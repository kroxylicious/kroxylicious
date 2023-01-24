/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.invoker;

import java.util.Arrays;

import io.kroxylicious.proxy.filter.KrpcFilter;

public class FilterInvokers {
    public static FilterInvoker invokerFor(KrpcFilter filter) {
        if (PerMessageInvoker.implementsAnyMessageInterface(filter)) {
            return new PerMessageInvoker(filter);
        }
        else {
            throw new IllegalArgumentException("Cannot build invoker for: " + filter.getClass().getSimpleName());
        }
    }

    public static FilterInvoker[] invokersFor(KrpcFilter[] filters) {
        return Arrays.stream(filters).map(FilterInvokers::invokerFor).toArray(FilterInvoker[]::new);
    }
}
