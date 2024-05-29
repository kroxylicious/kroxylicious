/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicLabellingTest {

    @Test
    void matches() {
        var tl = new TopicLabelling(Map.of("foo", "x"), List.of("abc"), List.of("xyy"), List.of("p+"));
        assertThat(tl.matches("abc")).isTrue();
        assertThat(tl.matches("abc1")).isFalse();
        assertThat(tl.matches("xyy")).isTrue();
        assertThat(tl.matches("xyyy")).isTrue();
        assertThat(tl.matches("xyyz")).isTrue();
        assertThat(tl.matches("xyx")).isFalse();
        assertThat(tl.matches("xyz")).isFalse();
        assertThat(tl.matches("p")).isTrue();
        assertThat(tl.matches("pq")).isFalse();
        assertThat(tl.matches("pp")).isTrue();
        assertThat(tl.matches("ppq")).isFalse();
        assertThat(tl.matches("p".repeat(12))).isTrue();
        assertThat(tl.matches("p".repeat(12) + "q")).isFalse();
    }

    @Test
    void definitelyNoTopicsInCommonWithNamedTopics() {
        var namedAbc = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of("abc"), List.of(), List.of());
        var namedXyz = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of("xyz"), List.of(), List.of());
        var namedAbcOrXyz = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of("abc", "xyz"), List.of(), List.of());

        assertThat(namedAbc.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(namedXyz.maybeSomeTopicsInCommon(namedXyz)).isTrue();
        assertThat(namedAbcOrXyz.maybeSomeTopicsInCommon(namedAbcOrXyz)).isTrue();

        assertThat(namedAbc.maybeSomeTopicsInCommon(namedXyz)).isFalse();
        assertThat(namedXyz.maybeSomeTopicsInCommon(namedAbc)).isFalse();

        assertThat(namedAbc.maybeSomeTopicsInCommon(namedAbcOrXyz)).isTrue();
        assertThat(namedAbcOrXyz.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(namedXyz.maybeSomeTopicsInCommon(namedAbcOrXyz)).isTrue();
        assertThat(namedAbcOrXyz.maybeSomeTopicsInCommon(namedXyz)).isTrue();
    }

    @Test
    void definitelyNoTopicsInCommonWithPrefixedTopics() {
        var namedAbc = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of("abc"), List.of(), List.of());
        var startsXyz = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of(), List.of("xyz"), List.of());
        var startsAbcOrXyz = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of(), List.of("abc", "xyz"), List.of());

        var startsAbc = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of(), List.of("abc"), List.of());
        var startsXyz1 = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of(), List.of("xyz1"), List.of());

        assertThat(namedAbc.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(startsXyz.maybeSomeTopicsInCommon(namedAbc)).isFalse();
        assertThat(startsAbcOrXyz.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(startsAbc.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(startsXyz1.maybeSomeTopicsInCommon(namedAbc)).isFalse();

        assertThat(namedAbc.maybeSomeTopicsInCommon(startsXyz)).isFalse();
        assertThat(startsXyz.maybeSomeTopicsInCommon(startsXyz)).isTrue();
        assertThat(startsAbcOrXyz.maybeSomeTopicsInCommon(startsXyz)).isTrue();
        assertThat(startsAbc.maybeSomeTopicsInCommon(startsXyz)).isFalse();
        assertThat(startsXyz1.maybeSomeTopicsInCommon(startsXyz)).isTrue();

        assertThat(namedAbc.maybeSomeTopicsInCommon(startsAbcOrXyz)).isTrue();
        assertThat(startsXyz.maybeSomeTopicsInCommon(startsAbcOrXyz)).isTrue();
        assertThat(startsAbcOrXyz.maybeSomeTopicsInCommon(startsAbcOrXyz)).isTrue();
        assertThat(startsAbc.maybeSomeTopicsInCommon(startsAbcOrXyz)).isTrue();
        assertThat(startsXyz1.maybeSomeTopicsInCommon(startsAbcOrXyz)).isTrue();

        assertThat(namedAbc.maybeSomeTopicsInCommon(startsAbc)).isTrue();
        assertThat(startsXyz.maybeSomeTopicsInCommon(startsAbc)).isFalse();
        assertThat(startsAbcOrXyz.maybeSomeTopicsInCommon(startsAbc)).isTrue();
        assertThat(startsAbc.maybeSomeTopicsInCommon(startsAbc)).isTrue();
        assertThat(startsXyz1.maybeSomeTopicsInCommon(startsAbc)).isFalse();

        assertThat(namedAbc.maybeSomeTopicsInCommon(startsXyz1)).isFalse();
        assertThat(startsXyz.maybeSomeTopicsInCommon(startsXyz1)).isTrue();
        assertThat(startsAbcOrXyz.maybeSomeTopicsInCommon(startsXyz1)).isTrue();
        assertThat(startsAbc.maybeSomeTopicsInCommon(startsXyz1)).isFalse();
        assertThat(startsXyz1.maybeSomeTopicsInCommon(startsXyz1)).isTrue();
    }

    @Test
    void definitelyNoTopicsInCommonWithRegex() {
        var matchesAbc = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of(), List.of(), List.of("abc"));
        var matchesXyz = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of(), List.of(), List.of("xyz"));
        var namedAbc = new TopicLabelling(
                Map.of("foo", "x",
                        "bar", "y"),
                List.of("abc"), List.of(), List.of());

        assertThat(matchesAbc.maybeSomeTopicsInCommon(matchesAbc)).isTrue();
        assertThat(matchesXyz.maybeSomeTopicsInCommon(matchesAbc)).isTrue();
        assertThat(namedAbc.maybeSomeTopicsInCommon(matchesAbc)).isTrue();

        assertThat(matchesAbc.maybeSomeTopicsInCommon(matchesXyz)).isTrue();
        assertThat(matchesXyz.maybeSomeTopicsInCommon(matchesXyz)).isTrue();
        assertThat(namedAbc.maybeSomeTopicsInCommon(matchesXyz)).isTrue();

        assertThat(matchesAbc.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(matchesXyz.maybeSomeTopicsInCommon(namedAbc)).isTrue();
        assertThat(namedAbc.maybeSomeTopicsInCommon(namedAbc)).isTrue();
    }

}