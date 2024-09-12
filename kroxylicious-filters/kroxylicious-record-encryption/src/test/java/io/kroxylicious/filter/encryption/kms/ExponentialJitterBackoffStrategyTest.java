/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.kms;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class ExponentialJitterBackoffStrategyTest {

    private static Stream<Arguments> testBackoffWithNoRandomJitter() {
        Duration ms500 = Duration.of(500, ChronoUnit.MILLIS);
        Duration ms800 = Duration.of(800, ChronoUnit.MILLIS);
        return Stream.of(
                Arguments.of(2d, ms500, 0, Duration.ZERO),
                Arguments.of(2d, ms500, 1, Duration.of(500, ChronoUnit.MILLIS)),
                Arguments.of(2d, ms500, 2, Duration.of(1, ChronoUnit.SECONDS)),
                Arguments.of(2d, ms500, 3, Duration.of(2, ChronoUnit.SECONDS)),
                // different initial delay
                Arguments.of(2d, ms800, 0, Duration.ZERO),
                Arguments.of(2d, ms800, 1, Duration.of(800, ChronoUnit.MILLIS)),
                Arguments.of(2d, ms800, 2, Duration.of(1600, ChronoUnit.MILLIS)),
                Arguments.of(2d, ms800, 3, Duration.of(3200, ChronoUnit.MILLIS)),
                // different multiplier
                Arguments.of(4d, ms500, 0, Duration.ZERO),
                Arguments.of(4d, ms500, 1, Duration.of(500, ChronoUnit.MILLIS)),
                Arguments.of(4d, ms500, 2, Duration.of(2000, ChronoUnit.MILLIS)),
                Arguments.of(4d, ms500, 3, Duration.of(8000, ChronoUnit.MILLIS))
        );
    }

    @ParameterizedTest
    @MethodSource
    void testBackoffWithNoRandomJitter(double multiplier, Duration initialDelay, int failures, Duration expected) {
        Random mockRandom = Mockito.mock(Random.class);
        when(mockRandom.nextLong()).thenReturn(0L);
        ExponentialJitterBackoffStrategy strategy = new ExponentialJitterBackoffStrategy(
                initialDelay,
                Duration.of(20, ChronoUnit.SECONDS),
                multiplier,
                mockRandom
        );
        Duration delay = strategy.getDelay(failures);
        assertThat(delay).isEqualTo(expected);
    }

    private static Stream<Arguments> testCappedToMaximumDelay() {
        Duration maxThreeSeconds = Duration.of(3, ChronoUnit.SECONDS);
        Duration maxFiveSeconds = Duration.of(5, ChronoUnit.SECONDS);
        return Stream.of(
                Arguments.of(maxThreeSeconds, 0, Duration.ZERO),
                Arguments.of(maxThreeSeconds, 1, Duration.of(1, ChronoUnit.SECONDS)),
                Arguments.of(maxThreeSeconds, 2, Duration.of(2, ChronoUnit.SECONDS)),
                Arguments.of(maxThreeSeconds, 3, Duration.of(3, ChronoUnit.SECONDS)),
                Arguments.of(maxThreeSeconds, 4, Duration.of(3, ChronoUnit.SECONDS)),
                // different maximum
                Arguments.of(maxFiveSeconds, 0, Duration.ZERO),
                Arguments.of(maxFiveSeconds, 1, Duration.of(1, ChronoUnit.SECONDS)),
                Arguments.of(maxFiveSeconds, 2, Duration.of(2, ChronoUnit.SECONDS)),
                Arguments.of(maxFiveSeconds, 3, Duration.of(4, ChronoUnit.SECONDS)),
                Arguments.of(maxFiveSeconds, 4, Duration.of(5, ChronoUnit.SECONDS)),
                Arguments.of(maxFiveSeconds, 5, Duration.of(5, ChronoUnit.SECONDS))
        );
    }

    @ParameterizedTest
    @MethodSource
    void testCappedToMaximumDelay(Duration maximum, int failures, Duration expected) {
        Random mockRandom = Mockito.mock(Random.class);
        when(mockRandom.nextLong()).thenReturn(0L);
        ExponentialJitterBackoffStrategy strategy = new ExponentialJitterBackoffStrategy(
                Duration.of(1, ChronoUnit.SECONDS),
                maximum,
                2,
                mockRandom
        );
        Duration delay = strategy.getDelay(failures);
        assertThat(delay).isEqualTo(expected);
    }

    private static Stream<Arguments> testRandomJitter() {
        return Stream.of(
                Arguments.of(0L, Duration.of(1000, ChronoUnit.MILLIS)),
                Arguments.of(200L, Duration.of(1200, ChronoUnit.MILLIS)),
                Arguments.of(999L, Duration.of(1999, ChronoUnit.MILLIS)),
                Arguments.of(1000L, Duration.of(1000, ChronoUnit.MILLIS)),
                Arguments.of(-200L, Duration.of(800, ChronoUnit.MILLIS)),
                Arguments.of(-999L, Duration.of(1, ChronoUnit.MILLIS)),
                Arguments.of(-1000L, Duration.of(1000, ChronoUnit.MILLIS))
        );
    }

    @ParameterizedTest
    @MethodSource
    void testRandomJitter(Long randomLong, Duration expected) {
        Random mockRandom = Mockito.mock(Random.class);
        when(mockRandom.nextLong()).thenReturn(randomLong);
        ExponentialJitterBackoffStrategy strategy = new ExponentialJitterBackoffStrategy(
                Duration.of(1, ChronoUnit.SECONDS),
                Duration.of(20, ChronoUnit.SECONDS),
                2,
                mockRandom
        );
        Duration delay = strategy.getDelay(1);
        assertThat(delay).isEqualTo(expected);
    }

    private static Stream<Arguments> invalidConfigurations() {
        Random random = Mockito.mock(Random.class);
        Duration negativeDuration = Duration.of(-3, ChronoUnit.SECONDS);
        Duration oneSecond = Duration.of(1, ChronoUnit.SECONDS);
        return Stream.of(
                Arguments.of(negativeDuration, oneSecond, 2.0d, random, "initialDelay must be greater than zero"),
                Arguments.of(Duration.ZERO, oneSecond, 2.0d, random, "initialDelay must be greater than zero"),
                Arguments.of(oneSecond, negativeDuration, 2.0d, random, "maximumDelay must be greater than zero"),
                Arguments.of(oneSecond, Duration.ZERO, 2.0d, random, "maximumDelay must be greater than zero"),
                Arguments.of(oneSecond, oneSecond, 0.0d, random, "multiplier must be greater than one"),
                Arguments.of(oneSecond, oneSecond, 1.0d, random, "multiplier must be greater than one"),
                Arguments.of(oneSecond, oneSecond, 2.0d, null, "random must be non-null"),
                Arguments.of(oneSecond, oneSecond, -1.2d, random, "multiplier must be greater than one")
        );
    }

    @ParameterizedTest
    @MethodSource
    void invalidConfigurations(Duration initial, Duration maximum, double multiplier, Random random, String message) {
        assertThatThrownBy(() -> {
            new ExponentialJitterBackoffStrategy(
                    initial,
                    maximum,
                    multiplier,
                    random
            );
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message);
    }

    @Test
    void testNegativeFailuresNotAllowed() {
        ExponentialJitterBackoffStrategy strategy = new ExponentialJitterBackoffStrategy(
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                2,
                new Random()
        );
        assertThatThrownBy(() -> {
            strategy.getDelay(-1);
        }).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("failures is negative");
    }

}
