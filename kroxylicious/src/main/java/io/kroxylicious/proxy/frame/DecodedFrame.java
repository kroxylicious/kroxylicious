/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.frame;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

/**
 * docco
 * @param <H>
 * @param <B>
 */
public interface DecodedFrame<H extends ApiMessage, B extends ApiMessage> {
    /**
     * docco
     * @return docco
     */
    short headerVersion();

    /**
     * docco
     * @return docco
     */
    H header();

    /**
     * docco
     * @return docco
     */
    B body();

    /**
     * docco
     * @return docco
     */
    ApiKeys apiKey();

    /**
     * docco
     * @return docco
     */
    short apiVersion();
}
