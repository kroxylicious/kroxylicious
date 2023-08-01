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
 * @param <FR> filter result
 */
public interface FilterResultBuilder<H extends ApiMessage, FR extends FilterResult> extends CloseStage<FR> {

    /**
     * A forward of a request or response to the next filter in the chain.
     *
     * @param header message header
     * @param message api message
     * @return next stage in the fluent builder API
     */
    CloseOrTerminalStage<FR> forward(H header, ApiMessage message);

    /**
     * Signals the desire of the filter that the connection is closed.
     *
     * @return last stage in the fluent API.
     */
    TerminalStage<FR> drop();

}
