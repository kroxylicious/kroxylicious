/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.datetime;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Duration specifier.
 * We use this in configuration to represent durations because:
 * 1. we want to preserve fidelity, that is it serializes to the exact same format.
 * 2. we don't want to install custom deserialization logic for Duration as it could
 *    collide with the jackson date time module in future.
 * @param amount amount
 * @param unit unit
 */
public record DurationSpec(long amount, ChronoUnit unit) {

    private static final Map<String, ChronoUnit> SUFFIX_UNIT = Map.of(
            "d", ChronoUnit.DAYS,
            "h", ChronoUnit.HOURS,
            "m", ChronoUnit.MINUTES,
            "s", ChronoUnit.SECONDS,
            "ms", ChronoUnit.MILLIS,
            "us", ChronoUnit.MICROS,
            "ns", ChronoUnit.NANOS);
    private static final Map<ChronoUnit, String> UNIT_SUFFIX = SUFFIX_UNIT.entrySet()
            .stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    private static final List<String> ALL_SUPPORTED_SUFFIXES = SUFFIX_UNIT.keySet().stream().sorted().toList();
    private static final List<ChronoUnit> ALL_SUPPORTED_UNITS = SUFFIX_UNIT.values().stream().sorted().toList();

    public DurationSpec {
        Objects.requireNonNull(unit);
        if (!UNIT_SUFFIX.containsKey(unit)) {
            throw new IllegalArgumentException("Unsupported unit " + unit + ". Must be one of " + ALL_SUPPORTED_UNITS);
        }
        if (amount < 0) {
            throw new IllegalArgumentException("Amount must be greater than or equal to zero");
        }
    }

    public Duration duration() {
        return Duration.of(amount, unit);
    }

    @NonNull
    @Override
    public String toString() {
        return amount + UNIT_SUFFIX.get(unit);
    }

    /**
     * Parse string form like '10s' into a DurationSpec
     * @param durationSpec string specifier
     * @throws IllegalArgumentException if durationSpec cannot be parsed
     * @return duration spec
     */
    public static DurationSpec parse(String durationSpec) {
        Objects.requireNonNull(durationSpec);
        if (durationSpec.length() < 2) {
            throw invalidDurationSpec(durationSpec, "Must be at least 2 characters long");
        }
        int firstNonNumeric = -1;
        int lastInvalidCharacter = -1;
        for (int i = 0; i < durationSpec.length(); i++) {
            if (!Character.isDigit(durationSpec.charAt(i)) && firstNonNumeric == -1) {
                firstNonNumeric = i;
            }
            if (!Character.isLetterOrDigit(durationSpec.charAt(i))) {
                lastInvalidCharacter = i;
            }
        }
        if (firstNonNumeric == -1) {
            throw invalidDurationSpec(durationSpec, "No unit discovered");
        }
        if (lastInvalidCharacter != -1) {
            throw invalidDurationSpec(durationSpec, "Invalid character '" + durationSpec.charAt(lastInvalidCharacter) + "' at index " + lastInvalidCharacter);
        }
        String unitSpecifier = durationSpec.substring(firstNonNumeric).toLowerCase(Locale.ROOT);
        if (!SUFFIX_UNIT.containsKey(unitSpecifier)) {
            throw invalidDurationSpec(durationSpec, "Unknown unit '" + unitSpecifier + "'. Must be one of " + ALL_SUPPORTED_SUFFIXES);
        }
        ChronoUnit unit = SUFFIX_UNIT.get(unitSpecifier);
        String amountSpec = durationSpec.substring(0, firstNonNumeric);
        try {
            long amount = Long.parseLong(amountSpec);
            return new DurationSpec(amount, unit);
        }
        catch (NumberFormatException e) {
            throw invalidDurationSpec(durationSpec, "Could not parse '" + amountSpec + "' as long. Must be an integer between 0 and " + Long.MAX_VALUE);
        }
    }

    @NonNull
    private static IllegalArgumentException invalidDurationSpec(String durationSpec, String reason) {
        return new IllegalArgumentException("Invalid duration spec: '" + durationSpec + "'. " + reason + ".");
    }
}
