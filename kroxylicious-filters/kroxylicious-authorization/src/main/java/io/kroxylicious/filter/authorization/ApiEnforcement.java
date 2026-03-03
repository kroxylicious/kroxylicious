/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResult;

/**
 * <p>Enforces authorization rules for a range of versions of a single API key.
 * It's usually the case that a single implementation is sufficient for a given API key, but
 * {@link CompositeEnforcement} can be used in cases where it's desirable to have
 * different implementations cover disjoint version ranges.</p>
 *
 * <p> If the request contains references
 * to access-controlled entities (such as topics) then {@link AuthorizationFilter#authorization(FilterContext, List)}
 * and {@link io.kroxylicious.authorizer.service.AuthorizeResult#partition(Collection, ResourceType, Function)}
 * should be used to partition the entities into a subset which can be forwarded to the broker, and a subset
 * which need to be rejected with some kind of authorization errors. When both of these sets are
 * non-empty it will be necessary to also handle the response from the broker to add the
 * locally generated subset of entities with authorization errors to the responses for the entities which
 * were forwarded to the broker.</p>
 *
 * <p>{@link AuthorizationFilter#pushInflightState(RequestHeaderData, InflightState)} can be used to
 * attach some state to a request that's going to be forwarded, and
 * {@link AuthorizationFilter#popAndApplyInflightState(ResponseHeaderData, Object)} is provided to easily
 * perform the merging of the locally-generated and upstream responses.</p>
 *
 * @param <Q> The request type.
 * @param <S> The response type.
 */
abstract class ApiEnforcement<Q extends ApiMessage, S extends ApiMessage> {

    /**
     * @return The inclusive minimum version of the range of versions supported
     */
    abstract short minSupportedVersion();

    /**
     * @return The inclusive maximum version of the range of versions supported
     */
    abstract short maxSupportedVersion();

    /**
     * <p>Performs the necessary authorization of the request.</p>
     * @param header The request header.
     * @param request The request body.
     * @param context The filter context.
     * @param authorizationFilter The authorization filter.
     * @return The filter result.
     */
    abstract CompletionStage<RequestFilterResult> onRequest(RequestHeaderData header,
                                                            Q request,
                                                            FilterContext context,
                                                            AuthorizationFilter authorizationFilter);

    /**
     * <p>Performs the necessary authorization or handling of the response.<p>
     * @param header The request header.
     * @param response The response body.
     * @param context The filter context.
     * @param authorizationFilter The authorization filter.
     * @return The filter result.
     */
    CompletionStage<ResponseFilterResult> onResponse(ResponseHeaderData header,
                                                     S response,
                                                     FilterContext context,
                                                     AuthorizationFilter authorizationFilter) {
        return context.forwardResponse(header, authorizationFilter.popAndApplyInflightState(header, response));
    }

}
