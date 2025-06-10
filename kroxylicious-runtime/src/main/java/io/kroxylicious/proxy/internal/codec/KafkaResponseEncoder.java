/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.frame.ResponseFrame;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaResponseEncoder extends KafkaMessageEncoder<ResponseFrame> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaResponseEncoder.class);

    public KafkaResponseEncoder(@Nullable KafkaMessageListener listener) {
        super(listener);
    }

    @Override
    protected Logger log() {
        return LOGGER;
    }

}
