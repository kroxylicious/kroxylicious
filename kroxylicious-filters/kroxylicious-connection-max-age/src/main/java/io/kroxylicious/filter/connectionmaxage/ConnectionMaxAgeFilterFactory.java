/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionmaxage;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.Plugins;

/**
 * A {@link FilterFactory} for {@link ConnectionMaxAgeFilter}.
 * <p>
 * Each call to {@link #createFilter} produces a filter with its own effective max age deadline,
 * randomized within the configured jitter range to avoid thundering herd reconnections.
 */
@Plugin(configType = ConnectionMaxAgeFilterConfig.class)
public class ConnectionMaxAgeFilterFactory
        implements FilterFactory<ConnectionMaxAgeFilterConfig, ConnectionMaxAgeFilterConfig> {

    private final Clock clock;

    /**
     * Default constructor used by the runtime.
     */
    public ConnectionMaxAgeFilterFactory() {
        this(Clock.systemUTC());
    }

    /**
     * Constructor allowing injection of a clock for testability.
     *
     * @param clock the clock to use
     */
    ConnectionMaxAgeFilterFactory(Clock clock) {
        this.clock = clock;
    }

    @Override
    public ConnectionMaxAgeFilterConfig initialize(FilterFactoryContext context,
                                                   ConnectionMaxAgeFilterConfig config) {
        return Plugins.requireConfig(this, config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context,
                               ConnectionMaxAgeFilterConfig configuration) {
        Duration effectiveMaxAge = computeEffectiveMaxAge(configuration);
        return new ConnectionMaxAgeFilter(effectiveMaxAge, clock);
    }

    private Duration computeEffectiveMaxAge(ConnectionMaxAgeFilterConfig config) {
        Duration maxAge = config.maxAgeDuration();
        Duration jitter = config.jitterDuration();
        if (jitter.isZero()) {
            return maxAge;
        }
        long jitterMillis = jitter.toMillis();
        long offsetMillis = ThreadLocalRandom.current().nextLong(-jitterMillis, jitterMillis + 1);
        Duration effective = maxAge.plusMillis(offsetMillis);
        // ensure the effective max age is at least 1ms
        if (effective.isNegative() || effective.isZero()) {
            effective = Duration.ofMillis(1);
        }
        return effective;
    }
}
