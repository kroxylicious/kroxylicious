/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public record ExecDecl(
                       @Nullable Path dir,
                       @Nullable Map<String, String> env,
                       @Nullable String command,
                       @Nullable List<String> args,
                       @Nullable Set<Integer> exitCodes,
                       @JsonDeserialize(using = DurationDeserializer.class) @Nullable Duration timeout,
                       @JsonDeserialize(using = DurationDeserializer.class) @Nullable Duration destroyTimeout,
                       @Nullable OutputFileAssertion standardOutput,
                       @Nullable OutputFileAssertion standardError) {

    public ExecDecl {
        if ((command == null) == (args == null)) {
            throw new IllegalArgumentException("One of command and args must be specified");
        }
    }

    public ExecDecl(@Nullable String command,
                    @Nullable List<String> args,
                    @Nullable Set<Integer> exitCodes,
                    @Nullable Duration timeout,
                    @Nullable Duration destroyTimeout,
                    @Nullable OutputFileAssertion standardOutput,
                    @Nullable OutputFileAssertion standardError) {
        this(null, null, command, args, exitCodes, timeout, destroyTimeout, standardOutput, standardError);
    }

    public @NonNull List<String> args() {
        if (args != null) {
            return args;
        }
        else if (command() != null) {
            return Arrays.asList(command().trim().split("\\s+"));
        }
        else {
            throw new IllegalStateException("One of command and args must be specified");
        }
    }

    public static class Deserializer extends JsonDeserializer<ExecDecl> {

        @Override
        public ExecDecl deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            if (jsonParser.isExpectedStartObjectToken()) {
                return jsonParser.readValueAs(ExecDecl.class);
            }
            else {
                String command = jsonParser.getText();
                return new ExecDecl(command, null, null, null, null, null, null);
            }
        }
    }

    @SuppressWarnings("java:S6353") // \\d may be more concise, but [0-9] is better known
    static final Pattern PATTERN = Pattern.compile("(?<num>[0-9]+)(?<unit>h|m|s|ms)");

    public static Duration parseDuration(String text) {
        var matcher = PATTERN.matcher(text);
        if (matcher.matches()) {
            long num = Long.parseLong(matcher.group("num"));
            return switch (matcher.group("unit")) {
                case "h" -> Duration.ofHours(num);
                case "m" -> Duration.ofMinutes(num);
                case "s" -> Duration.ofSeconds(num);
                case "ms" -> Duration.ofMillis(num);
                default -> throw new IllegalStateException("Unexpected unit " + matcher.group("unit"));
            };
        }
        throw new IllegalArgumentException("Invalid duration '" + text + "', should match " + PATTERN.pattern());
    }

    public static class DurationDeserializer extends JsonDeserializer<Duration> {

        @Override
        public Duration deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {
            String text = jsonParser.getText();
            return parseDuration(text);
        }
    }

}
