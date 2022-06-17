/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.kroxylicious.proxy.frame.ResponseFrame;

public class KafkaResponseEncoder extends KafkaMessageEncoder<ResponseFrame> {
    private static final Logger LOGGER = LogManager.getLogger(KafkaResponseEncoder.class);

    @Override
    protected Logger log() {
        return LOGGER;
    }
}
