/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;

import edu.umd.cs.findbugs.annotations.Nullable;

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
     * The generated error response is API-specific: it carries the given {@code error}'s code, and
     * the error's default message, either at the top level of the response (if the API for the
     * response has a global error code), or for all entities given in the request (if the API for
     * the response has only per-entity error codes).
     * @param header the headers from the request
     * @param requestMessage the API request message to generate an error in response too.
     * @param error the error to convey to the client; its {@link Errors#code() code} is set on the
     *              generated response and its {@link Errors#message() default message} is used.
     *              Must denote an actual error; {@link Errors#NONE} is not permitted.
     * @return next stage in the fluent builder API
     * @throws IllegalArgumentException header or message do not meet criteria described above, or
     *         {@code error} is {@link Errors#NONE}.
     */
    CloseOrTerminalStage<RequestFilterResult> errorResponse(RequestHeaderData header,
                                                            ApiMessage requestMessage,
                                                            Errors error)
            throws IllegalArgumentException;

    /**
     * Generate a short-circuit error response towards the client.
     * The generated error response is API-specific: it carries the given {@code error}'s code, and
     * the given message, either at the top level of the response (if the API for the response has a
     * global error code), or for all entities given in the request (if the API for the response has
     * only per-entity error codes).
     * @param header the headers from the request
     * @param requestMessage the API request message to generate an error in response too.
     * @param error the error to convey to the client; its {@link Errors#code() code} is set on the
     *              generated response. Must denote an actual error; {@link Errors#NONE} is not permitted.
     * @param message the error message to convey to the client, or {@code null} to use the error's
     *                default message.
     * @return next stage in the fluent builder API
     * @throws IllegalArgumentException header or message do not meet criteria described above, or
     *         {@code error} is {@link Errors#NONE}.
     */
    CloseOrTerminalStage<RequestFilterResult> errorResponse(RequestHeaderData header,
                                                            ApiMessage requestMessage,
                                                            Errors error,
                                                            @Nullable String message)
            throws IllegalArgumentException;

}
