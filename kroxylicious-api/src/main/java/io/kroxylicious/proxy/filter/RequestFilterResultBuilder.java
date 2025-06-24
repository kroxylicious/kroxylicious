/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import javax.annotation.Nullable;

import org.apache.kafka.common.errors.ApiException;
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
     * @param header response header. May be null.
     * @param message response message. May not be null.  the response messages the class must have one
     *                that ends with ResponseData.
     * @return next stage in the fluent builder API
     * @throws IllegalArgumentException header or message do not meet criteria described above.
     */
    CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(@Nullable ResponseHeaderData header, ApiMessage message) throws IllegalArgumentException;

    /**
     * A short-circuit response towards the client.
     *
     * @param message response message. May not be null.  the response messages the class must have one
     *                that ends with ResponseData.
     * @return next stage in the fluent builder API
     * @throws IllegalArgumentException header or message do not meet criteria described above.
     */
    CloseOrTerminalStage<RequestFilterResult> shortCircuitResponse(ApiMessage message) throws IllegalArgumentException;

    /**
     * Generate a short-circuit error response towards the client.
     * The generated error response is API-specific, and add an error code (corresponding to the ApiException), and possibly error message (from the message of the ApiException), either at the top level of the response (if the API for the response has a global error code), or for all entities given in the request (if the API for the response has only-per entity error codes).
     * @param header the headers from the request
     * @param message the api message to generate an error in response too.
     * @param apiException the exception that triggered the error response. Note Kafka will map the exception to an {@see org.apache.kafka.common.requests.ApiError} using {@see org.apache.kafka.common.protocol.Errors#forException(java.lang.Throwable)} so callers may wish to supply choose their exception to trigger the appropriate error code
     * @return next stage in the fluent builder API
     * @throws IllegalArgumentException header or message do not meet criteria described above.
     */
    CloseOrTerminalStage<RequestFilterResult> errorResponse(RequestHeaderData header,
                                                            ApiMessage message,
                                                            ApiException apiException)
            throws IllegalArgumentException;

}
