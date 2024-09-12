/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer.sasl;

import java.time.Duration;

public interface BackoffStrategy {

    /**
     * Decides how long to delay the next attempt at an action given it has attempted
     * for N consecutive times in the past.
     * @param attempts count of attempts, must be 0 or greater
     * @return how long to delay
     * @throws IllegalArgumentException if failures less than 0
     */
    Duration getDelay(int attempts);
}
