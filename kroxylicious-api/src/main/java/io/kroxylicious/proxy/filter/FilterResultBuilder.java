/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseStage;
import io.kroxylicious.proxy.filter.filterresultbuilder.TerminalStage;

/**
 * Fluent builder for filter results.
 *
 * @param <H>  kafka api message header class
 * @param <R> filter result
 */
public interface FilterResultBuilder<H extends ApiMessage, R extends FilterResult> extends CloseStage<R> {

    /**
     * A forward of a request or response message to the next filter in the chain.
     *
     * @param header message header. May not be null.
     * @param message api message. May not be null.  for request messages the class must have a name
     *                that that ends with RequestData. for response messages the class must have one
     *                that ends with ResponseData.
     * @return next stage in the fluent builder API
     * @throws IllegalArgumentException header or message do not meet criteria described above.
     */
    CloseOrTerminalStage<R> forward(H header, ApiMessage message) throws IllegalArgumentException;

    /**
     * Signals the desire of the filter that the connection is closed.
     *
     * @return last stage in the fluent API.
     */
    TerminalStage<R> drop();

}
