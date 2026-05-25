/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe clock whose time can be advanced programmatically.
 */
class MutableClock extends Clock {

    private final AtomicLong millis;
    private final ZoneId zone;

    MutableClock(Instant start) {
        this(start, ZoneOffset.UTC);
    }

    private MutableClock(Instant start,
                         ZoneId zone) {
        this.millis = new AtomicLong(start.toEpochMilli());
        this.zone = zone;
    }

    void advance(Duration duration) {
        millis.addAndGet(duration.toMillis());
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new MutableClock(Instant.ofEpochMilli(millis.get()), zone);
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis.get());
    }

    @Override
    public long millis() {
        return millis.get();
    }
}
