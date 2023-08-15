/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 *
 * <h2 id='implementing'>Implementing Filters</h2>
 *
 * <h3 id='implementing.filterResults'>Filter Results</h3>
 * <p>Filter implementation must return a {@link java.util.concurrent.CompletionStage} containing a
 * {@link io.kroxylicious.proxy.filter.FilterResult} object. It is the job of FilterResult to convey what
 * message is to be forwarded to the next filter in the chain (or client/broker if at the chain's beginning
 * or end).  It is also used to carry instructions such as indicating that the connection must be closed,
 * or a message dropped.</p>
 * <p>If the filter returns a CompletionStage that is already completed normally, Kroxylicious will immediately
 * perform the action described by the FilterResult.</p>
 * <p>If the CompletionStage completes exceptionally, the connection is closed.  This also applies if the
 * CompletionStage does not complete within a timeout (20000 milliseconds).</p>
 * <h3 id='implementing.deferringForward'>Deferring Forwards</h3>
 * <p>The filter may return a CompletionStage that is not yet completed. When this happens, Kroxylicious will pause
 * reading from the downstream (the Client writes will eventually block), and it begins to queue up in-flight
 * requests/responses arriving at the filter.  This is done so that message order is maintained.  Once the
 * CompletionStage completes, the action described  by the FilterResult is performed, reading from the downstream
 * resumes and any queued up requests/responses are processed.</p>
 * <p><strong>IMPORTANT:</strong> The pausing of reads from the downstream is a relatively costly operation.  To maintain optimal performance
 * filter implementations should minimise the occasions on which an incomplete CompletionStage is returned.</p>
 * <h3 id='implementing.createFilterResults'>Creating Filter Result objects</h3>
 * <p>The {@link io.kroxylicious.proxy.filter.KrpcFilterContext} is the factory for the FilterResult objects.</p>
 * <p>There are two convenience methods that simply allow a filter to immediately forward a result:</p>
 * <ul>
 *     <li>{@link io.kroxylicious.proxy.filter.KrpcFilterContext#forwardRequest(org.apache.kafka.common.message.RequestHeaderData, org.apache.kafka.common.protocol.ApiMessage)}, and</li>
 *     <li>{@link io.kroxylicious.proxy.filter.KrpcFilterContext#forwardResponse(org.apache.kafka.common.message.ResponseHeaderData, org.apache.kafka.common.protocol.ApiMessage)}.</li>
 * </ul>
 * <p>To access richer features, use the filter result builders:</p>
 * <ul>
 *     <li>{@link io.kroxylicious.proxy.filter.KrpcFilterContext#requestFilterResultBuilder()} and</li>
 *     <li>{@link io.kroxylicious.proxy.filter.KrpcFilterContext#responseFilterResultBuilder()}.</li>
 * </ul>
 */
package io.kroxylicious.proxy.filter;
