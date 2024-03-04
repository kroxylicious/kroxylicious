/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;

public interface BackoffStrategy {

    /**
     * Decides how long to delay the next attempt at an action given it has failed
     * for N consecutive times in the past.
     * @param failures count of failures, must be 0 or greater
     * @return how long to delay
     * @throws IllegalArgumentException if failures less than 0
     */
    Duration getDelay(int failures);
}
