/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.ApiMessage;

/**
 * Encapsulates the action a Filter wants to command the framework to take in response to handling a
 * message. That could be to forward a request or response to the next filter in the chain. Or it could
 * command the framework to close the connection or drop the frame. Or some combination like forward
 * and close connection.
 *
 * @see FilterCommandBuilder
 */
public interface FilterCommand {
    /**
     * the header to be forwarded to the next filter in the chain.
     *
     * @return header.
     */
    ApiMessage header();

    /**
     * the message to be forwarded to the next filter in the chain.
     * @return header.
     */
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
