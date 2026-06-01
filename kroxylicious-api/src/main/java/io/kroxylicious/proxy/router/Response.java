/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * A response received from a receiver (backing cluster or nested router).
 */
public interface Response {

    /**
     * @return the response header
     */
    ResponseHeaderData header();

    /**
     * @return the response body
     */
    ApiMessage body();
}
