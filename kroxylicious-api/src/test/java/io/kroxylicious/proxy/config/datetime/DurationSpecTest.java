/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.datetime;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class DurationSpecTest {

    public static Stream<Arguments> parseValid() {
        return Stream.of(argumentSet("days", "5d", new DurationSpec(5L, ChronoUnit.DAYS)),
                argumentSet("hours", "44h", new DurationSpec(44L, ChronoUnit.HOURS)),
                argumentSet("0 amount", "0h", new DurationSpec(0L, ChronoUnit.HOURS)),
                argumentSet("long max amount", Long.MAX_VALUE + "h", new DurationSpec(Long.MAX_VALUE, ChronoUnit.HOURS)),
                argumentSet("minutes", "3m", new DurationSpec(3L, ChronoUnit.MINUTES)),
                argumentSet("seconds", "6s", new DurationSpec(6L, ChronoUnit.SECONDS)),
                argumentSet("milliseconds", "85ms", new DurationSpec(85L, ChronoUnit.MILLIS)),
                argumentSet("microseconds", "25us", new DurationSpec(25L, ChronoUnit.MICROS)),
                argumentSet("nanoseconds", "1ns", new DurationSpec(1L, ChronoUnit.NANOS)));
    }

    @MethodSource
    @ParameterizedTest
    void parseValid(String input, DurationSpec expected) {
        DurationSpec parse = DurationSpec.parse(input);
        assertThat(parse).isEqualTo(expected);
        assertThat(parse.toString()).isEqualTo(input);
    }

    public static Stream<Arguments> parseInvalid() {
        return Stream.of(argumentSet("unknown unit", "5z", "Unknown unit 'z'. Must be one of [d, h, m, ms, ns, s, us]"),
                argumentSet("shorter than minimum", "5", "Must be at least 2 characters long"),
                argumentSet("no unit", "500", "No unit discovered"),
                argumentSet("preceding whitespace", " 500", "Invalid character ' ' at index 0"),
                argumentSet("trailing whitespace", "500 ", "Invalid character ' ' at index 3"),
                argumentSet("intervening whitespace", "500 m", "Invalid character ' ' at index 3"),
                argumentSet("larger than max long", Long.MAX_VALUE + "1m",
                        "Could not parse '92233720368547758071' as long. Must be an integer between 0 and 9223372036854775807"),
                argumentSet("negative amount", "-5m", "Invalid character '-' at index 0"));
    }

    @MethodSource
    @ParameterizedTest
    void parseInvalid(@Nullable String input, String reason) {
        assertThatThrownBy(() -> DurationSpec.parse(input)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid duration spec: '" + input + "'. " + reason + ".");
    }

    @Test
    void duration() {
        DurationSpec spec = new DurationSpec(5, ChronoUnit.HOURS);
        assertThat(spec.duration()).isEqualTo(Duration.of(5, ChronoUnit.HOURS));
    }

    @Test
    void durationAmountMustBePositive() {
        assertThatThrownBy(() -> new DurationSpec(-1, ChronoUnit.SECONDS)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Amount must be greater than or equal to zero");
    }

    @Test
    void durationUnitMustBeKnown() {
        assertThatThrownBy(() -> new DurationSpec(-1, ChronoUnit.MONTHS)).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsupported unit Months. Must be one of [Nanos, Micros, Millis, Seconds, Minutes, Hours, Days]");
    }

    @Test
    void durationUnitMustBeNonNull() {
        assertThatThrownBy(() -> new DurationSpec(-1, null)).isInstanceOf(NullPointerException.class);
    }

}