/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.time.Clock;

/**
 * Exposes package-private clock override methods on {@link TopicPartitionRouterFactory}
 * to integration tests in other packages.
 */
public class TopicPartitionRouterFactoryTestSupport {

    private TopicPartitionRouterFactoryTestSupport() {
    }

    public static void setClockOverride(Clock clock) {
        TopicPartitionRouterFactory.setClockOverride(clock);
    }

    public static void clearClockOverride() {
        TopicPartitionRouterFactory.clearClockOverride();
    }
}
