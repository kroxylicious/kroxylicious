/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * <p>The protocol filter API.</p>
 *
 * <p>An interface is provided for each kind of request and response in the Kafka protocol, e.g. {@link io.kroxylicious.proxy.filter.ProduceRequestFilter}.
 * Protocol Filter implementations inherit whichever of the per-RPC interfaces they need to intercept.
 * They can inherit multiple interfaces if necessary.
 * For filters which needs to intercept most or all of the protocol it is more convenient to inherit
 * {@link io.kroxylicious.proxy.filter.RequestFilter} and/or {@link io.kroxylicious.proxy.filter.ResponseFilter}.</p>
 *
 * <h2 id='assumptions'>Important facts about the Kafka protocol</h2>
 *
 * <h3 id='pipelining'>Pipelining</h3>
 * <p>The Kafka protocol supports pipelining (meaning a client can send multiple requests,
 * before getting a response for any of them). Therefore when writing a filter implementation
 * do not assume you won't see multiple requests before seeing any corresponding responses.</p>
 *
 * <h3 id='ordering'>Ordering</h3>
 * <p>A broker does not, in general, send responses in the same order as it receives requests.
 * Therefore when writing a filter implementation do not assume ordering.</p>
 *
 * <h3 id='local_view'>Local view</h3>
 * <p>A client may obtain information from one broker in a cluster and use it to interact with other
 * brokers in the cluster (or the same broker, but on a different connection, and therefore a different
 * channel and filter chain). A classic example would
 * be a producer or consumer making a metadata connection and {@code Metadata} request to a broker and
 * then connecting to a partition leader to producer/consume records ({@code Produce} and {@code Fetch} requests).</p>
 *
 * <p>So although your filter
 * <em>implementation</em> might intercept both {@code Metadata} and {@code Produce} request/response
 * (for example), those requests will not pass through the same <em>instance</em> of your filter
 * implementation. Therefore it is incorrect, in general, to assume your filter has a global view of
 * the communication between the client and broker.</p>
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
 * <p>The {@link io.kroxylicious.proxy.filter.FilterContext} is the factory for the FilterResult objects.</p>
 * <p>There are two convenience methods that simply allow a filter to immediately forward a result:</p>
 * <ul>
 *     <li>{@link io.kroxylicious.proxy.filter.FilterContext#forwardRequest(org.apache.kafka.common.message.RequestHeaderData, org.apache.kafka.common.protocol.ApiMessage)}, and</li>
 *     <li>{@link io.kroxylicious.proxy.filter.FilterContext#forwardResponse(org.apache.kafka.common.message.ResponseHeaderData, org.apache.kafka.common.protocol.ApiMessage)}.</li>
 * </ul>
 * <p>To access richer features, use the filter result builders:</p>
 * <ul>
 *     <li>{@link io.kroxylicious.proxy.filter.FilterContext#requestFilterResultBuilder()} and</li>
 *     <li>{@link io.kroxylicious.proxy.filter.FilterContext#responseFilterResultBuilder()}.</li>
 * </ul>
 * <h3 id='implementing.threadSafety'>Thread Safety</h3>
 * <p>
 * The Filter API provides the following thread-safety guarantees:
 * </p>
 * <ol>
 *   <li>There is a single thread associated with each connection and this association lasts for the lifetime of connection..</li>
 *   <li>Each filter instance is associated with exactly one connection.</li>
 *   <li>Construction of the filter instance and dispatch of the filter methods {@code onXxxRequest} and
 *       {@code onXxxResponse} takes place on that same thread.</li>
 *   <li>Any computation stages chained to the {@link java.util.concurrent.CompletionStage} returned by
 *       {@link io.kroxylicious.proxy.filter.FilterContext#sendRequest(org.apache.kafka.common.message.RequestHeaderData, org.apache.kafka.common.protocol.ApiMessage)}
 *       using the default execution methods (using methods without the suffix async) or default asynchronous execution
 *       (using methods with suffix async that employ the stage's default asynchronous execution facility)
 *       are guaranteed to be performed by that same thread.  Computation stages chained using custom asynchronous
 *       execution (using methods with suffix async that take an Executor argument) do not get this guarantee.</li>
 *  </ol>
 *  <p>Filter implementations are free to rely on these guarantees to safely maintain state within fields
 *     of the Filter without employing additional synchronization.</p>
 *  <p>If a Filter needs to do some asynchronous work and mutate members of the Filter, they can access
 *  the thread for the connection via {@link io.kroxylicious.proxy.filter.FilterFactoryContext#filterDispatchExecutor()},
 *  which is available in {@link io.kroxylicious.proxy.filter.FilterFactory#createFilter(FilterFactoryContext, java.lang.Object)}
 *  when it is creating an instance of a Filter. Ensure that any work executes quickly as this is the IO thread for potentially
 *  many connections.</p>
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.filter;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;