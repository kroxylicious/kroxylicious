/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.oauthbearer.sasl;

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

    public ExponentialJitterBackoffStrategy(
            @NonNull
            Duration initialDelay,
            @NonNull
            Duration maximumDelay,
            double multiplier,
            Random random
    ) {
        if (multiplier <= 1.0d) {
            throw new IllegalArgumentException("multiplier must be greater than one");
        }
        if (initialDelay.compareTo(Duration.ZERO) <= 0) {
            throw new IllegalArgumentException("initialDelay must be greater than zero");
        }
        if (maximumDelay.compareTo(Duration.ZERO) <= 0) {
            throw new IllegalArgumentException("maximumDelay must be greater than zero");
        }
        if (random == null) {
            throw new IllegalArgumentException("random must be non-null");
        }
        this.initialDelay = initialDelay;
        this.maximumDelay = maximumDelay;
        this.multiplier = multiplier;
        this.random = random;
    }

    @Override
    public Duration getDelay(int attempts) {
        if (attempts < 0) {
            throw new IllegalArgumentException("attempts is negative");
        }
        if (attempts == 0) {
            return Duration.ZERO;
        }
        Duration backoff = getExponentialBackoff(attempts);
        backoff = backoff.plus(getRandomJitter(attempts, backoff));
        return backoff.compareTo(maximumDelay) < 0 ? backoff : maximumDelay;
    }

    private Duration getRandomJitter(int attempts, Duration backoff) {
        Duration prior = getExponentialBackoff(attempts - 1);
        long maxJitter = backoff.toMillis() - prior.toMillis();
        return maxJitter == 0 ? Duration.ZERO : Duration.ofMillis(this.random.nextLong() % maxJitter);
    }

    private Duration getExponentialBackoff(int attempts) {
        if (attempts == 0) {
            return Duration.ZERO;
        }
        return Duration.ofMillis((long) (initialDelay.toMillis() * (Math.pow(multiplier, (double) attempts - 1))));
    }
}
