/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;

public class DurationSerde {

    /**
     * Prevent construction of utility class
     */
    private DurationSerde() {
        // prevent construction
    }

    private record Unit(ChronoUnit unit, String groupName, String serializedUnit) {

        public Duration duration(long amount) {
            try {
                return Duration.of(amount, unit);
            }
            catch (ArithmeticException e) {
                throw new DurationArithmeticException(amount + " " + unit + " could not be converted to a Duration", e);
            }
        }
    }

    private static final List<Unit> UNITS = List.of(new Unit(ChronoUnit.DAYS, "days", "d"),
            new Unit(ChronoUnit.HOURS, "hours", "h"),
            new Unit(ChronoUnit.MINUTES, "minutes", "m"),
            new Unit(ChronoUnit.SECONDS, "seconds", "s"),
            new Unit(ChronoUnit.MILLIS, "millis", "ms"),
            new Unit(ChronoUnit.MICROS, "micros", "μs"),
            new Unit(ChronoUnit.NANOS, "nanos", "ns"));

    public static class Deserializer extends StdScalarDeserializer<Duration> {

        // note that micros is special, allowing μs or us
        private static final Pattern PATTERN = Pattern.compile("(?<sign>-)?(?:(?<days>\\d+)d)?(?:(?<hours>\\d+)h)?(?:(?<minutes>\\d+)m)?"
                + "(?:(?<seconds>\\d+)s)?(?:(?<millis>\\d+)ms)?(?:(?<micros>\\d+)[μu]s)?(?:(?<nanos>\\d+)ns)?");

        private static final String USAGE = "Expected a string time duration such as \"1h30m\", or \"120s\"; supported units are d, h, m, s, ms, μs (or us) and ns.";

        public Deserializer() {
            super(Duration.class);
        }

        @Override
        public Duration deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.hasToken(JsonToken.VALUE_STRING)) {
                String text = p.getText();
                Matcher matcher = getMatcherIfValid(p, text);
                boolean negated = matcher.group("sign") != null;
                try {
                    return UNITS.stream().flatMap(unit -> Optional.ofNullable(matcher.group(unit.groupName()))
                            .stream()
                            .map(maybeLong -> {
                                try {
                                    long parsedLong = Long.parseLong(maybeLong);
                                    return unit.duration(parsedLong);
                                }
                                catch (NumberFormatException e) {
                                    throw new NumberFormatException("Invalid duration string: '" + maybeLong + "', could not be parsed as long");
                                }
                            })).reduce(Duration.ZERO, negated ? DurationSerde::minus : DurationSerde::plus);
                }
                catch (DurationArithmeticException e) {
                    throw new JsonParseException(p, "Invalid duration string: '" + text + "'. Likely it is too large to be converted to Duration: " + e.getMessage(), e);
                }
            }
            else {
                throw new JsonParseException(p, "Invalid serialized duration. Expected a string value, but was " + p.currentToken());
            }
        }

        private Matcher getMatcherIfValid(JsonParser p, String text) throws JsonParseException {
            if (text.isBlank()) {
                throw patternMismatchException(p, text);
            }
            if (text.equals("-")) {
                throw patternMismatchException(p, text);
            }
            Matcher matcher = PATTERN.matcher(text);
            if (!matcher.matches()) {
                throw patternMismatchException(p, text);
            }
            return matcher;
        }

        private JsonParseException patternMismatchException(JsonParser p, String text) {
            return new JsonParseException(p, "Invalid duration string: '" + text + "'. " + USAGE);
        }
    }

    private static Duration minus(Duration a, Duration b) {
        try {
            return a.minus(b);
        }
        catch (ArithmeticException e) {
            throw new DurationArithmeticException(a + " minus " + b + " failed", e);
        }
    }

    private static Duration plus(Duration a, Duration b) {
        try {
            return a.plus(b);
        }
        catch (ArithmeticException e) {
            throw new DurationArithmeticException(a + " plus " + b + " failed", e);
        }
    }

    public static class Serializer extends StdScalarSerializer<Duration> {

        public Serializer() {
            super(Duration.class);
        }

        @Override
        public void serialize(Duration value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            if (value.isZero()) {
                gen.writeString("0ms");
            }
            else {
                StringBuilder result = new StringBuilder();
                Duration temp = value;
                if (temp.isNegative()) {
                    result.append("-");
                }
                for (Unit unit : UNITS) {
                    Duration oneUnit = unit.duration(1);
                    long wholeUnits = temp.dividedBy(oneUnit);
                    temp = temp.minus(wholeUnits, unit.unit());
                    if (wholeUnits != 0) {
                        result.append(Math.abs(wholeUnits));
                        result.append(unit.serializedUnit());
                    }
                }
                gen.writeString(result.toString());
            }
        }
    }
}
