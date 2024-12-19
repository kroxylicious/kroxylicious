/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources;

import java.time.Duration;

import io.strimzi.api.kafka.model.kafka.Kafka;

import io.kroxylicious.systemtests.Constants;

/**
 * The type Resource operation.
 */
public class ResourceOperation {
    /**
     * Gets timeout for resource readiness.
     *
     * @return the timeout for resource readiness
     */
    public static Duration getTimeoutForResourceReadiness() {
        return getTimeoutForResourceReadiness("default");
    }

    /**
     * Gets timeout for resource readiness.
     *
     * @param kind the kind
     * @return the timeout for resource readiness
     */
    public static Duration getTimeoutForResourceReadiness(String kind) {
        return switch (kind) {
            case Kafka.RESOURCE_KIND -> Duration.ofMinutes(10);
            case Constants.DEPLOYMENT -> Duration.ofMinutes(6);
            default -> Duration.ofMinutes(3);
        };
    }

    /**
     * timeoutForPodsOperation returns a reasonable timeout in milliseconds for a number of Pods in a quorum to roll on update,
     *  scale up or create
     * @param numberOfPods the number of pods
     * @return the long
     */
    public static long timeoutForPodsOperation(int numberOfPods) {
        return Duration.ofMinutes(5).toMillis() * Math.max(1, numberOfPods);
    }

    /**
     * Gets timeout for resource deletion.
     *
     * @return the timeout for resource deletion
     */
    public static long getTimeoutForResourceDeletion() {
        return getTimeoutForResourceDeletion("default");
    }

    /**
     * Gets timeout for resource deletion.
     *
     * @param kind the kind
     * @return the timeout for resource deletion
     */
    public static long getTimeoutForResourceDeletion(String kind) {
        return switch (kind) {
            case Kafka.RESOURCE_KIND, Constants.POD_KIND -> Duration.ofMinutes(5).toMillis();
            default -> Duration.ofMinutes(3).toMillis();
        };
    }
}
