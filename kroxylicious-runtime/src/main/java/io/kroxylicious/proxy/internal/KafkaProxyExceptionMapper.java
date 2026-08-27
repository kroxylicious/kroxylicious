/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

import io.kroxylicious.proxy.frame.DecodedRequestFrame;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * In the operation of the proxy there are various exceptions which are "anticipated" but not necessarily handled directly.
 * SSL Handshake errors are an illustrative example, where they are thrown and propagate through the netty channel without being handled.
 * <p>
 * The exception mapper provides a mechanism to register specific exceptions with function to evaluate them and if appropriate generate a message to respond to the
 * client with.
 */
public class KafkaProxyExceptionMapper {

    private KafkaProxyExceptionMapper() {
    }

    /**
     * Builds the body of an error response answering the given request frame, with error codes
     * set according to the given error.
     * @param frame the request frame being answered
     * @param error the error to convey to the client
     * @return the error response body
     */
    @Nullable
    public static ApiMessage errorResponseMessage(DecodedRequestFrame<?> frame, Throwable error) {
        Errors errors = Errors.forException(error);
        return ErrorResponseFactory.errorResponseData(frame.apiKey(), frame.body(), frame.apiVersion(), errors, error.getMessage());
    }

    /**
     * Builds an error response answering the given request message, with error codes set
     * according to the given exception.
     * @param requestHeaders the headers of the request being answered
     * @param message the body of the request being answered
     * @param apiException the exception to convey to the client
     * @return the error response
     */
    @Nullable
    public static ApiMessage errorResponseForMessage(RequestHeaderData requestHeaders, ApiMessage message, ApiException apiException) {
        ApiKeys apiKey = ApiKeys.forId(message.apiKey());
        Errors errors = Errors.forException(apiException);
        return ErrorResponseFactory.errorResponseData(apiKey, message, requestHeaders.requestApiVersion(), errors, apiException.getMessage());
    }
}
