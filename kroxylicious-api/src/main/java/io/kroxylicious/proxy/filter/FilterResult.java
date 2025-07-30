/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.ApiMessage;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The result of a filter request or response operation that encapsulates the request or response
 * to be forwarded to the next filter in the chain.  Optionally it carries orders for actions such
 * as close the connection or drop the message.
 *
 * @see FilterResultBuilder
 */
public interface FilterResult {
    /**
     * the header to be forwarded to the next filter in the chain.
     *
     * @return header.
     */
    @Nullable
    ApiMessage header();

    /**
     * the message to be forwarded to the next filter in the chain.
     * @return header.
     */
    @Nullable
    ApiMessage message();

    /**
     * signals the filter's wish that the connection will be closed. if the filter provides a
     * message it will be forwarded before the connection is closed.
     * @return true if the connection is to be closed.
     */
    boolean closeConnection();

    /**
     * signals the filter's wish that message is dropped i.e. not forward to the next filter
     * in the chain.
     * @return true if message is to be dropped.
     */
    boolean drop();
}
