/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * A specialization of the {@link FilterResult} for request filters.
 * <br>
 * A request filter may, rather than forwarding a request towards the broker, opt to
 * send a response towards the client instead. This is called a short circuit response. It
 * is useful for implementing validating filters.
 */
public interface RequestFilterResult extends FilterResult {

    /**
     * If true, the request is carrying a short circuit response.
     *
     * @return true if carrying a short circuit response.
     */
    boolean shortCircuitResponse();
}
