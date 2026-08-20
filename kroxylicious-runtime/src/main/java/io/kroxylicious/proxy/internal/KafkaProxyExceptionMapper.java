/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestAndSize;

import io.netty.buffer.Unpooled;

import io.kroxylicious.kafka.common.errors.ApiException;
import io.kroxylicious.kafka.common.message.ListConfigResourcesResponseData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import io.kroxylicious.kafka.common.protocol.MessageUtil;
import io.kroxylicious.proxy.frame.DecodedRequestFrame;
import io.kroxylicious.proxy.internal.codec.BodyDecoder;
import io.kroxylicious.proxy.internal.codec.ByteBufAccessorImpl;
import io.kroxylicious.proxy.tag.VisibleForTesting;

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
    public static ApiMessage errorResponseMessage(DecodedRequestFrame<?> frame, Throwable error) {
        return errorResponse(frame, error);
    }

    private static ListConfigResourcesResponseData newListConfigResourcesV0ErrorResponse(Errors error) {
        return new ListConfigResourcesResponseData()
                .setErrorCode(error.code())
                .setConfigResources(List.of(new ListConfigResourcesResponseData.ConfigResource().setResourceType((byte) 16)));
    }

    /**
     * Builds an error response answering the given request message, with error codes set
     * according to the given exception.
     * @param requestHeaders the headers of the request being answered
     * @param message the body of the request being answered
     * @param apiException the exception to convey to the client
     * @return the error response
     */
    public static ApiMessage errorResponseForMessage(RequestHeaderData requestHeaders, ApiMessage message, ApiException apiException) {
        final short apiKey = message.apiKey();
        // Our ListConfigResourcesRequestData is deserialized off the wire and so will only have the v0 fields populated.
        // we can't just call errorResponse, which in turn uses a ListConfigResourcesRequest.Builder,
        // because that builder applies some validation that's inappropriate for our purposes.
        if (apiKey == ApiKeys.LIST_CONFIG_RESOURCES.id && requestHeaders.requestApiVersion() == 0) {
            return newListConfigResourcesV0ErrorResponse(Errors.forException(apiException));
        }
        return buildErrorResponse(ApiKeys.forId(apiKey), message, requestHeaders.requestApiVersion(), apiException);
    }

    @VisibleForTesting
    static ApiMessage errorResponse(DecodedRequestFrame<?> frame, Throwable error) {
        ApiMessage reqBody = frame.body();
        short apiVersion = frame.apiVersion();
        final ApiKeys apiKey = frame.apiKey();
        if (apiKey == ApiKeys.LIST_CONFIG_RESOURCES && apiVersion == 0) {
            return newListConfigResourcesV0ErrorResponse(Errors.forException(error));
        }
        return buildErrorResponse(apiKey, reqBody, apiVersion, error);
    }

    /*
     * Kafka doesn't offer any nicely-abstracted code for building an error response with error codes set
     * appropriately for an arbitrary request type. Instead we round-trip the vendored request body through
     * kafka-clients: serialize it, parse it back as a kafka-clients AbstractRequest (which knows how to build
     * its own error response), serialize that response, then decode it back into the vendored response type.
     * Cost is an extra serialize/parse pair, paid only on the error path.
     */
    private static ApiMessage buildErrorResponse(ApiKeys vendoredApiKey, ApiMessage reqBody, short apiVersion, Throwable error) {
        var kafkaApiKey = org.apache.kafka.common.protocol.ApiKeys.forId(vendoredApiKey.id);
        ByteBuffer requestBytes = MessageUtil.toByteBufferAccessor(reqBody, apiVersion).buffer();
        RequestAndSize ras = AbstractRequest.parseRequest(kafkaApiKey, apiVersion,
                new org.apache.kafka.common.protocol.ByteBufferAccessor(requestBytes));
        short code = Errors.forException(error).code();
        var kafkaException = org.apache.kafka.common.protocol.Errors.forCode(code).exception();
        AbstractResponse kafkaResponse = ras.request.getErrorResponse(kafkaException);
        ByteBuffer responseBytes = org.apache.kafka.common.protocol.MessageUtil
                .toByteBufferAccessor(kafkaResponse.data(), apiVersion).buffer();
        return BodyDecoder.decodeResponse(vendoredApiKey, apiVersion,
                new ByteBufAccessorImpl(Unpooled.wrappedBuffer(responseBytes)));
    }
}
