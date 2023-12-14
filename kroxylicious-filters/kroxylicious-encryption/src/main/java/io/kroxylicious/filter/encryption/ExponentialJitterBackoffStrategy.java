/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.Random;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ExponentialJitterBackoffStrategy implements BackoffStrategy {

    @NonNull
    private final Duration initialDelay;
    @NonNull
    private final Duration maximumDelay;
    private final double multiplier;
    private final Random random;

    public ExponentialJitterBackoffStrategy(@NonNull Duration initialDelay,
                                            @NonNull Duration maximumDelay,
                                            double multiplier,
                                            Random random) {
        this.initialDelay = initialDelay;
        this.maximumDelay = maximumDelay;
        this.multiplier = multiplier;
        this.random = random;
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier should not reduce the initial delay");
        }

    }

    @Override
    public Duration getDelay(int failures) {
        if (failures == 0) {
            return Duration.ZERO;
        }
        Duration backoff = getExponentialBackoff(failures);
        backoff = backoff.plus(getRandomJitter(failures, backoff));
        return backoff.compareTo(maximumDelay) < 0 ? backoff : maximumDelay;
    }

    private Duration getRandomJitter(int failures, Duration backoff) {
        Duration prior = getExponentialBackoff(failures - 1);
        long maxJitter = backoff.toMillis() - prior.toMillis();
        return Duration.ofMillis(this.random.nextLong() % maxJitter);
    }

    private Duration getExponentialBackoff(int failures) {
        if (failures == 0) {
            return Duration.ZERO;
        }
        return Duration.ofMillis((long) (initialDelay.toMillis() * (Math.pow(multiplier, failures - 1))));
    }
}
