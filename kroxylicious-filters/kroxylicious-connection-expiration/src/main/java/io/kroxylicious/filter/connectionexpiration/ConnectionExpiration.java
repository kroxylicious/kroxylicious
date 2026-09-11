/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionexpiration;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongBinaryOperator;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link FilterFactory} for {@link ConnectionExpirationFilter}.
 * <p>
 * Each call to {@link #createFilter} produces a filter with its own effective expiration deadline,
 * randomized within the configured jitter range to avoid thundering herd reconnections.
 */
@Plugin(configType = ConnectionExpirationFilterConfig.class)
public class ConnectionExpiration
        implements FilterFactory<ConnectionExpirationFilterConfig, ConnectionExpirationFilterConfig> {

    private final Clock clock;

    private final LongBinaryOperator randomSource;

    /**
     * Default constructor used by the runtime.
     */
    public ConnectionExpiration() {
        this(Clock.systemUTC());
    }

    /**
     * Constructor allowing injection of a clock for testability.
     *
     * @param clock the clock to use
     */
    @VisibleForTesting
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Pseudorandomness sufficient for generating jitter; not security relevant
    @SuppressWarnings("java:S2245") // Pseudorandomness sufficient for generating jitter; not security relevant
    ConnectionExpiration(Clock clock) {
        this(clock, ThreadLocalRandom.current()::nextLong);
    }

    /**
     * Constructor allowing injection of a clock and random source for testability.
     *
     * @param clock the clock to use
     * @param randomSource a function returning a random long in [origin, bound)
     */
    @VisibleForTesting
    ConnectionExpiration(Clock clock, LongBinaryOperator randomSource) {
        this.clock = clock;
        this.randomSource = randomSource;
    }

    @Override
    public ConnectionExpirationFilterConfig initialize(FilterFactoryContext context,
                                                       ConnectionExpirationFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context,
                               @NonNull ConnectionExpirationFilterConfig configuration) {
        Duration effectiveMaxAge = computeEffectiveMaxAge(configuration);
        return new ConnectionExpirationFilter(effectiveMaxAge, clock);
    }

    private Duration computeEffectiveMaxAge(ConnectionExpirationFilterConfig config) {
        Duration maxAge = config.maxAge();
        Duration jitter = config.jitter() == null ? Duration.ZERO : config.jitter();
        if (jitter.isZero()) {
            return maxAge;
        }
        long jitterMillis = jitter.toMillis();
        long offsetMillis = randomSource.applyAsLong(-jitterMillis, jitterMillis + 1);
        Duration effective = maxAge.plusMillis(offsetMillis);
        // ensure the effective max age is at least 1ms
        if (effective.isNegative() || effective.isZero()) {
            effective = Duration.ofMillis(1);
        }
        return effective;
    }
}
