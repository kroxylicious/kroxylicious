/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;

/**
 * Builder for request filter results.
 * <br/>
 * See {@link RequestFilterResult} for a description of short-circuit responses.
 */
public interface RequestFilterResultBuilder extends FilterResultBuilder<RequestHeaderData, RequestFilterResult> {

    /**
     * A short-circuit response towards the client.
     *
     * @param header response header
     * @param message response message
     * @return next stage in the fluent builder API
     */
    CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(ResponseHeaderData header, ApiMessage message);

    /**
     * A short-circuit response towards the client.
     *
     * @param message response message
     * @return next stage in the fluent builder API
     */
    CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(ApiMessage message);

}
