/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * <p>The API for writing protocol filters (aka interceptors).</p>
 *
 * <p>{@link io.kroxylicious.proxy.filter.KrpcRequestFilter} is the base interface for request filters,
 * with a subinterface for each request API (e.g. {@link io.kroxylicious.proxy.filter.ProduceRequestFilter}).
 * Similarly {@link io.kroxylicious.proxy.filter.KrpcResponseFilter} is the base interface for response filters,
 * with a subinterface for each response API (e.g. {@link io.kroxylicious.proxy.filter.ProduceResponseFilter}).</p>
 *
 * <p>Filter plugins can multiply inherit several of the per-RPC interfaces if they need to intercept several kinds
 * of request and/or response.</p>
 *
 * <p>Filter plugins exist in a chain that is specific to a single channel. The
 * <p>filter chain is instantiated by a {@link io.kroxylicious.proxy.internal.FilterChainFactory}
 * when a connection is made by a client.</p>
 *
 * <h3 id='assumptions'>Important facts about the Kafka protocol</h3>
 *
 * <h4 id='pipelining'>Pipelining</h4>
 * <p>The Kafka protocol supports pipelining (meaning a client can send multiple requests,
 * before getting a response for any of them). Therefore when writing a filter implementation
 * do not assume you won't see multiple requests before seeing any corresponding responses.</p>
 *
 * <h4 id='ordering'>Ordering</h4>
 * <p>A broker does not, in general, send responses in the same order as it receives requests.
 * Therefore when writing a filter implementation do not assume ordering.</p>
 *
 * <h4 id='local_view'>Local view</h4>
 * <p>A client may obtain information from one broker in a cluster and use it to interact with other
 * brokers in the cluster (or the same broker, but on a different connection, and therefore a different
 * channel and filter chain). A classic example would
 * be a producer or consumer making a metadata connection and {@code Metadata} request to a broker and
 * then connecting to a partition leader to producer/consume records ({@code Produce} and {@code Fetch} requests).</p>
 *
 * <p>So although your filter
 * <em>implementation</em> might intercept both {@code Metadata} and {@code Produce} request/response
 * (for example), those requests will not pass through the same <emp>instance</emp> of your filter
 * implementation. Therefore it is incorrect, in general, to assume your filter has a global view of
 * the communication between the client and broker.</p>
 *
 */
package io.kroxylicious.proxy.filter;