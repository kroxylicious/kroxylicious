/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public record UtcClock(Clock clock) {
    public static UtcClock of(Clock clock) {
        return new UtcClock(clock);
    }

    public ZonedDateTime now() {
        return ZonedDateTime.ofInstant(clock.instant(), ZoneId.of(ZoneOffset.UTC.getId()));
    }
}
