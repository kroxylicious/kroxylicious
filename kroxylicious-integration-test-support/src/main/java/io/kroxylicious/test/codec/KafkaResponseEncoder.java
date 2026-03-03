/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaResponseEncoder
 */
public class KafkaResponseEncoder extends KafkaMessageEncoder<Frame> {

    /**
     * Create KafkaResponseEncoder
     */
    public KafkaResponseEncoder() {
        // explicit default constructor for javadoc
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseEncoder.class);

    @Override
    protected Logger log() {
        return LOGGER;
    }

}
