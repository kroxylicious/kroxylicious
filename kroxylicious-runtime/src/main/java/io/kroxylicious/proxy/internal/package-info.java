/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * <b>io.kroxylicious.proxy.internal</b> contains Kroxylicious internal code implementing Proxying functionality using Netty.
 * <h2>Understanding the Kroxylicious Pipeline</h2>
 * The Kroxylicious pipeline for a client connection is composed of:
 * <ul>
 *     <li>A {@link io.kroxylicious.proxy.internal.KafkaProxyInitializer} that initialises the downstream channel pipeline between client and kroxylicious</li>
 *     <li>A {@link io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler} that establishes the upstream channel pipeline to the broker and forwards requests
 *     from the downstream channel to the upstream channel</li>
 *     <li>A {@link io.kroxylicious.proxy.internal.KafkaProxyBackendHandler} that writes responses read from the upstream channel back to the downstream channel</li>
 * </ul>
 * <h3>KafkaProxyInitializer</h3>
 * {@link io.kroxylicious.proxy.internal.KafkaProxyInitializer} is the ChannelInitializer for Kroxylicious. It is responsible for
 * installing Handlers into the pipeline to implement behaviours including:
 * <ul>
 *    <li>Decode SNI hostname</li>
 *    <li>Resolve VirtualCluster for the channel</li>
 *    <li>Decode Kafka Request messages</li>
 *    <li>Encode Kafka Response messages</li>
 *    <li>{@link io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler} to handle Kroxylicious business logic</li>
 * </ul>
 * Note that for this downstream Channel, the inbound direction carries Requests (we read requests from the channel) and the outbound direction carries Responses from the
 * backend server (response are written to the channel). So Handlers are invoked first-to-last for Requests, and last-to-first for Responses.
 * <h3>KafkaProxyFrontendHandler</h3>
 * {@link io.kroxylicious.proxy.internal.KafkaProxyFrontendHandler} handles the proxy lifecycle, it:
 * <ul>
 *    <li>Selects a backend server to proxy to using the {@link io.kroxylicious.proxy.filter.NetFilter} API</li>
 *    <li>Initiates a Channel connection to the selected backend server</li>
 *    <li>Creates a {@link io.kroxylicious.proxy.internal.KafkaProxyBackendHandler}</li>
 *    <li>Writes messages read from the downstream channel to the upstream channel</li>
 *    <li>Installs handlers into the backend channel pipeline, including the Users configured Custom Protocol Filters as well as Kafka Request Encoding and Response Decoding handlers</li>
 *    <li>Configures a predicate for the channel, based on the installed Filters, to determine when to decode Kafka messages</li>
 * </ul>
 * Note that for the upstream Channel, the outbound direction carries Requests (we write requests to the channel) and the inbound direction carries Responses from the
 * backend server (we read responses from the channel). So Filters are installed into the pipeline in the reverse order that they are declared in the Kroxylicious configuration
 * YAML. Pipeline Handlers are invoked last-to-first for Requests, and first-to-last for Responses.
 * <h3>KafkaProxyBackendHandler</h3>
 * {@link io.kroxylicious.proxy.internal.KafkaProxyBackendHandler} signals to the Frontend Handler when the upstream channel is ready to be written to, and it writes
 * Responses to the downstream channel after it has read them from the upstream channel.
 */
@ReturnValuesAreNonnullByDefault
@DefaultAnnotationForParameters(NonNull.class)
@DefaultAnnotation(NonNull.class)
package io.kroxylicious.proxy.internal;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.DefaultAnnotationForParameters;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.ReturnValuesAreNonnullByDefault;