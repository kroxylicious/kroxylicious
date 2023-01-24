/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.ApiKeys;

/**
 * <p>Interface for {@code *RequestFilter}s.
 * This interface is not usually implemented directly.
 * Instead filter classes can (multiply) implement one of the RPC-specific subinterfaces such
 * as {@link ProduceRequestFilter} for a type-safe API.
 *
 * <p>When implementing one or more of the {@code *RequestFilter} subinterfaces you need only implement
 * the {@code on*Request} method(s), unless your filter can avoid deserialization in which case
 * you can override {@link #shouldDeserializeRequest(ApiKeys, short)} as well.</p>
 *
 * <h2>Guarantees</h2>
 * <p>Implementors of this API may assume the following:</p>
 * <ol>
 *     <li>That each instance of the filter is associated with a single channel</li>
 *     <li>That filters are applied in the order they were configured.</li>
 * </ol>
 * <p>From 1. and 2. it follows that you can use member variables in your filter to
 * store channel-local state.</p>
 *
 * <p>Implementors should <strong>not</strong> assume:</p>
 * <ol>
 *     <li>That filters in the same chain execute on the same thread. Thus inter-filter communication/state
 *     transfer needs to be thread-safe</li>
 * </ol>
 */
public /* sealed */ interface KrpcFilter /* TODO permits ... */ {

    /**
     * <p>Determines whether a request with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * <p>Note that if you implement this method in a Per-Message filter then it will only be invoked for the ApiKey
     * targeted. i.e if you implement a ProduceRequestFilter then all non-Produce-request messages are filtered without
     * calling this method.</p><p>
     * Note that it is not guaranteed that this method will be called once per request,
     * or that two consecutive calls refer to the same request.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldDeserializeRequest} on request A</li>
     *     <li>{@code shouldDeserializeRequest} on request B</li>
     *     <li>{@code shouldDeserializeRequest} on request A</li>
     *     <li>{@code apply} on request A</li>
     *     <li>{@code apply} on request B</li>
     * </ol>
     *
     * @param apiKey     The API key
     * @param apiVersion The API version
     * @return whether the request should be serialized
     */
    default boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion) {
        return true;
    }

    /**
     * <p>Determines whether a response with the given {@code apiKey} and {@code apiVersion} should be deserialized.
     * <p>Note that if you implement this method in a Per-Message filter then it will only be invoked for the ApiKey
     * targeted. i.e if you implement a ProduceResponseFilter then all non-Produce-response messages are filtered without
     * calling this method.</p><p>
     * Note that it is not guaranteed that this method will be called once per response,
     * or that two consecutive calls refer to the same response.
     * That is, the sequences of invocations like the following are allowed:</p>
     * <ol>
     *     <li>{@code shouldDeserializeResponse} on response A</li>
     *     <li>{@code shouldDeserializeResponse} on response B</li>
     *     <li>{@code shouldDeserializeResponse} on response A</li>
     *     <li>{@code apply} on response A</li>
     *     <li>{@code apply} on response B</li>
     * </ol>
     *
     * @param apiKey     The API key
     * @param apiVersion The API version
     * @return whether the response should be deserialized
     */
    default boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
        return true;
    }

}
