/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class DurationSerdeTest {

    private static final Duration ONE_OF_EACH_UNIT = Duration.ofDays(1).plusHours(1).plusMinutes(1).plusSeconds(1).plusMillis(1).plus(1, ChronoUnit.MICROS).plusNanos(1);
    private static final Duration NEGATIVE_ONE_OF_EACH_UNIT = Duration.ofDays(-1).plusHours(-1).plusMinutes(-1).plusSeconds(-1).plusMillis(-1).plus(-1, ChronoUnit.MICROS)
            .plusNanos(-1);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String EXPECTED_USAGE = "Expected a string time duration such as \"1h30m\", or \"120s\"; supported units are d, h, m, s, ms, μs (or us) and ns.";

    static Stream<Arguments> deserializeValid() {
        return Stream.of(argumentSet("days", "8d", Duration.ofDays(8)),
                argumentSet("hours", "1h", Duration.ofHours(1)),
                argumentSet("minutes", "5m", Duration.ofMinutes(5)),
                argumentSet("seconds", "20s", Duration.ofSeconds(20)),
                argumentSet("90 minutes", "90m", Duration.ofMinutes(90)),
                argumentSet("3600 seconds", "3600s", Duration.ofHours(1)),
                argumentSet("multiple units of same duration", "1h60m3600s", Duration.ofHours(3)),
                argumentSet("milliseconds", "7ms", Duration.ofMillis(7)),
                argumentSet("microseconds", "1μs", Duration.of(1, ChronoUnit.MICROS)),
                argumentSet("microseconds (alternate)", "5us", Duration.of(5, ChronoUnit.MICROS)),
                argumentSet("zeros", "0d0h0m0s0ms0us0ns", Duration.ZERO),
                argumentSet("negative", "-1d1h1m1s1ms1us1ns",
                        NEGATIVE_ONE_OF_EACH_UNIT),
                argumentSet("complex", "1d1h1m1s1ms1us1ns",
                        ONE_OF_EACH_UNIT),
                argumentSet("nanoseconds", "3ns", Duration.ofNanos(3)),
                argumentSet("max possible duration", "106751991167300d15h30m7s999ms999μs999ns", Duration.ofSeconds(Long.MAX_VALUE, 999_999_999)),
                argumentSet("min possible duration", "-106751991167300d15h30m8s", Duration.ofSeconds(Long.MIN_VALUE, 0)));
    }

    private record TestRecord(@JsonDeserialize(using = DurationSerde.Deserializer.class) @JsonSerialize(using = DurationSerde.Serializer.class) Duration duration) {

    }

    @MethodSource
    @ParameterizedTest
    void deserializeValid(String durationString, Duration expected) throws IOException {
        TestRecord testRecord = MAPPER.reader().readValue("""
                {"duration": "%s"}
                """.formatted(durationString), TestRecord.class);
        assertThat(testRecord.duration()).isEqualTo(expected);
    }

    static Stream<Arguments> deserializeInvalid() {
        return Stream.of(argumentSet("number", "55", "Invalid serialized duration. Expected a string value, but was VALUE_NUMBER_INT"),
                argumentSet("array", "[]", "Invalid serialized duration. Expected a string value, but was START_ARRAY"),
                argumentSet("object", "{}", "Invalid serialized duration. Expected a string value, but was START_OBJECT"),
                argumentSet("empty", "\"\"", "Invalid duration string: ''. " + EXPECTED_USAGE),
                argumentSet("blank", "\" \"", "Invalid duration string: ' '. " + EXPECTED_USAGE),
                argumentSet("larger than long max", "\"" + Long.MAX_VALUE + "1d\"",
                        "Invalid duration string: '92233720368547758071', could not be parsed as long"),
                argumentSet("overflow max duration", "\"106751991167301d\"",
                        "Invalid duration string: '106751991167301d'. Likely it is too large to be converted to Duration: 106751991167301 Days could not be converted to a Duration"),
                argumentSet("overflow max negative duration", "\"-106751991167301d\"",
                        "Invalid duration string: '-106751991167301d'. Likely it is too large to be converted to Duration: 106751991167301 Days could not be converted to a Duration"),
                argumentSet("overflow max duration during addition", "\"106751991167300d1000h\"",
                        "Invalid duration string: '106751991167300d1000h'. Likely it is too large to be converted to Duration: PT2562047788015200H plus PT1000H failed"),
                argumentSet("overflow max duration during subtraction", "\"-106751991167300d1000h\"",
                        "Invalid duration string: '-106751991167300d1000h'. Likely it is too large to be converted to Duration: PT-2562047788015200H minus PT1000H failed"),
                argumentSet("units not descending - seconds before hours", "\"1s1h\"", "Invalid duration string: '1s1h'. " + EXPECTED_USAGE),
                argumentSet("units not descending - minutes before hours", "\"1m1h\"", "Invalid duration string: '1m1h'. " + EXPECTED_USAGE),
                argumentSet("units not descending - hours before days", "\"1h1d\"", "Invalid duration string: '1h1d'. " + EXPECTED_USAGE),
                argumentSet("units not descending - micros before millis", "\"1us1ms\"", "Invalid duration string: '1us1ms'. " + EXPECTED_USAGE),
                argumentSet("sign only", "\"-\"", "Invalid duration string: '-'. " + EXPECTED_USAGE),
                argumentSet("preceding whitespace", "\" 1d\"", "Invalid duration string: ' 1d'. " + EXPECTED_USAGE),
                argumentSet("trailing whitespace", "\"1d \"", "Invalid duration string: '1d '. " + EXPECTED_USAGE),
                argumentSet("intervening whitespace", "\"1 d\"", "Invalid duration string: '1 d'. " + EXPECTED_USAGE));
    }

    @MethodSource
    @ParameterizedTest
    void deserializeInvalid(String durationString, String expectedMessage) {
        String input = """
                {"duration": %s}
                """.formatted(durationString);
        ObjectReader reader = MAPPER.reader();
        assertThatThrownBy(() -> {
            reader.readValue(input, TestRecord.class);
        }).isInstanceOf(JsonMappingException.class).hasMessageContaining(expectedMessage);
    }

    public static Stream<Arguments> serialize() {
        return Stream.of(argumentSet("days", Duration.ofDays(8), """
                {"duration":"8d"}\
                """),
                argumentSet("hours", Duration.ofHours(5), """
                        {"duration":"5h"}\
                        """),
                argumentSet("minutes", Duration.ofMinutes(6), """
                        {"duration":"6m"}\
                        """),
                argumentSet("seconds", Duration.ofSeconds(7), """
                        {"duration":"7s"}\
                        """),
                argumentSet("millis", Duration.ofMillis(9), """
                        {"duration":"9ms"}\
                        """),
                argumentSet("micros", Duration.of(7, ChronoUnit.MICROS), """
                        {"duration":"7μs"}\
                        """),
                argumentSet("nanos", Duration.ofNanos(1), """
                        {"duration":"1ns"}\
                        """),
                argumentSet("complex", ONE_OF_EACH_UNIT, """
                        {"duration":"1d1h1m1s1ms1μs1ns"}\
                        """),
                argumentSet("complex negative", NEGATIVE_ONE_OF_EACH_UNIT, """
                        {"duration":"-1d1h1m1s1ms1μs1ns"}\
                        """),
                argumentSet("zero", Duration.ZERO, """
                        {"duration":"0ms"}\
                        """),
                argumentSet("max possible duration", Duration.ofSeconds(Long.MAX_VALUE, 999_999_999), """
                        {"duration":"106751991167300d15h30m7s999ms999μs999ns"}\
                        """),
                argumentSet("min possible duration", Duration.ofSeconds(Long.MIN_VALUE, 0), """
                        {"duration":"-106751991167300d15h30m8s"}\
                        """),
                argumentSet("uses largest units", Duration.ofSeconds(60), """
                        {"duration":"1m"}\
                        """));
    }

    @MethodSource
    @ParameterizedTest
    void serialize(Duration duration, String expectedSerialized) throws IOException {
        String serialized = MAPPER.writeValueAsString(new TestRecord(duration));
        assertThat(serialized).isEqualTo(expectedSerialized);
    }
}