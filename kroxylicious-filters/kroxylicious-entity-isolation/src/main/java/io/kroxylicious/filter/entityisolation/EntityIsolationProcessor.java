/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.entityisolation;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

import edu.umd.cs.findbugs.annotations.UnknownNullness;

/**
 * An entity isolation processor for a request/response pair.
 *
 * @param <Q> request
 * @param <S> response
 * @param <C> request context or void
 */
interface EntityIsolationProcessor<Q extends ApiMessage, S extends ApiMessage, C> {

    /**
     * @return The inclusive minimum version of the range of versions supported
     */
    short minSupportedVersion();

    /**
     * @return The inclusive maximum version of the range of versions supported
     */
    short maxSupportedVersion();

    /**
     * tests whether the apiVersion is beyond the range described by
     * minSupportedVersion...maxSupportedVersion.
     * @return true if out of range
     */
    default boolean versionIsOutOfRange(short apiVersion) {
        return apiVersion < minSupportedVersion() || apiVersion > maxSupportedVersion();
    }

    /**
     * Returns true if the processor should isolate this request version.
     * @param apiVersion version
     * @return  true if the processor should isolate this request version.
     */
    default boolean shouldHandleRequest(short apiVersion) {
        return false;
    }

    /**
     * Creates correlation object for the given request, if required. Processors
     * that do not require correlation must return null.
     * <br/>
     * Implementations are guaranteed that this method will be called before onRequest is
     * invoked.
     * <br/>
     * If an implementation wishes to use the request itself as the correlation, it is its responsibility
     * to perform a deep copy and return copy.
     *
     * @param request request
     * @return correlation object or null.
     */
    @UnknownNullness
    default C createCorrelatedRequestContext(Q request) {
        return null;
    }

    /**
     * Performs isolation on the given request object.
     *
     * @param header header
     * @param apiVersion version
     * @param request request
     * @param filterContext filter context
     * @param mapperContext mapping context
     * @return stage containing request filter result.
     */
    default CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                           short apiVersion,
                                                           Q request,
                                                           FilterContext filterContext,
                                                           MapperContext mapperContext) {
        return filterContext.forwardRequest(header, request);
    }

    /**
     * Returns true if the processor should isolate this response version.
     * @param apiVersion version
     * @return  true if the processor should isolate this request version.
     */
    default boolean shouldHandleResponse(short apiVersion) {
        return false;
    }

    /**
     * Performs isolation on the given response object.
     * @param header header
     * @param apiVersion api version
     * @param correlatedRequestContext correlated request object, or null.
     * @param response response
     * @param filterContext filter context
     * @param mapperContext mapping context
     * @return stage containing response filter result.
     */
    default CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                             short apiVersion,
                                                             @UnknownNullness C correlatedRequestContext,
                                                             S response,
                                                             FilterContext filterContext,
                                                             MapperContext mapperContext) {
        return filterContext.forwardResponse(header, response);
    }
}
