/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

/**
 * <p>Marker interface for {@code *RequestFilter}s and {@code *ResponseFilter}.
 * This interface must not be implemented directly.
 * Instead, filter classes (multiply) implement one of the RPC-specific subinterfaces such
 * as {@link ProduceRequestFilter}, {@link FetchResponseFilter}.
 *
 * <p>When implementing one or more of the {@code *RequestFilter} or {@code *ResponseFilter}
 * subinterfaces you can annotate the {@code on*Request} or {@code on*Response} methods
 * with {@link ApiVersions} if the filter
 * only needs to intercept a subset of the API versions for that API.</p>
 *
 * <h3>Guarantees</h3>
 * <p>Implementors of this API may assume the following:</p>
 * <ol>
 *     <li>That each instance of the filter is associated with a single channel</li>
 *     <li>That filters are applied in the order they were installed via 
 *     {@link io.kroxylicious.proxy.filter.NetFilter.NetFilterContext#initiateConnect(String, int, KrpcFilter[])}.</li>
 * </ol>
 * <p>From 1. it follows that you can use member variables in your filter to
 * store channel-local state.</p>
 *
 * <p>Implementors should <strong>not</strong> assume:</p>
 * <ol>
 *     <li>That filters in the same chain execute on the same thread. Thus inter-filter communication/state
 *     transfer needs to be thread-safe</li>
 * </ol>
 */
public /* sealed */ interface KrpcFilter /* TODO permits ... */ {

}
