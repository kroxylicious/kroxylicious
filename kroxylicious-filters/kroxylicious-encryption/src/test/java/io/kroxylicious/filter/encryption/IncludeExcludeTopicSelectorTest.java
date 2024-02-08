/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

class IncludeExcludeTopicSelectorTest {
    static List<EnvelopeEncryption.TopicSelectorConfig> defaultConfigurations() {
        return List.of(
                getConfig(null, null, null, null),
                getConfig(List.of(), List.of(), List.of(), List.of()));
    }

    @MethodSource("defaultConfigurations")
    @ParameterizedTest
    void testAnyTopicSelectedByDefault(EnvelopeEncryption.TopicSelectorConfig config) {
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("arbitrary")).isTrue();
    }

    @Test
    void testInclusionByName() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(List.of("included"), null, null, null);
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("included")).isTrue();
        assertThat(selector.select("badman")).isFalse();
    }

    @Test
    void testExclusionByName() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, List.of("excluded"), null, null);
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("excluded")).isFalse();
        assertThat(selector.select("arbitrary")).isTrue();
    }

    @Test
    void testInclusionByPrefix() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, null, List.of("prefix_"), null);
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("prefix_a")).isTrue();
        assertThat(selector.select("prefix_b")).isTrue();
        assertThat(selector.select("badman")).isFalse();
    }

    @Test
    void testExclusionByPrefix() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, null, null, List.of("prefix_"));
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("prefix_a")).isFalse();
        assertThat(selector.select("prefix_b")).isFalse();
        assertThat(selector.select("arbitrary")).isTrue();
    }

    @Test
    void testInclusionByPrefixOrName() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(List.of("included"), null, List.of("prefix_"), null);
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("included")).isTrue();
        assertThat(selector.select("prefix_a")).isTrue();
        assertThat(selector.select("prefix_b")).isTrue();
        assertThat(selector.select("badman")).isFalse();
    }

    @Test
    void testExclusionByPrefixOrName() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, List.of("excluded"), null, List.of("prefix_"));
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("excluded")).isFalse();
        assertThat(selector.select("prefix_a")).isFalse();
        assertThat(selector.select("prefix_b")).isFalse();
        assertThat(selector.select("arbitrary")).isTrue();
    }

    @Test
    void testExclusionBeatsInclusion() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(List.of("excluded"), List.of("excluded"), null, null);
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("excluded")).isFalse();
    }

    @Test
    void testIncludePrefixWithExactExclusion() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, List.of("prefix_2"), List.of("prefix_"), null);
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("prefix_1")).isTrue();
        assertThat(selector.select("prefix_2")).isFalse();
        assertThat(selector.select("prefix_3")).isTrue();
        assertThat(selector.select("arbitrary")).isFalse();
    }

    @Test
    void testIncludePrefixWithPrefixExclusion() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, null, List.of("prefix_"), List.of("prefix_2"));
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("prefix_1")).isTrue();
        assertThat(selector.select("prefix_2")).isFalse();
        assertThat(selector.select("prefix_21")).isFalse();
        assertThat(selector.select("prefix_3")).isTrue();
        assertThat(selector.select("arbitrary")).isFalse();
    }

    @Test
    void testIncludePrefixWithPrefixAndExactExclusion() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, List.of("prefix_2"), List.of("prefix_"), List.of("prefix_3"));
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("prefix_1")).isTrue();
        assertThat(selector.select("prefix_2")).isFalse();
        assertThat(selector.select("prefix_21")).isTrue();
        assertThat(selector.select("prefix_31")).isFalse();
        assertThat(selector.select("prefix_4")).isTrue();
        assertThat(selector.select("arbitrary")).isFalse();
    }

    @Test
    void testExcludePrefixBeatsIncludePrefix() {
        EnvelopeEncryption.TopicSelectorConfig config = getConfig(null, null, List.of("prefix_"), List.of("prefix_"));
        TopicSelector selector = new IncludeExcludeTopicSelector(config);
        assertThat(selector.select("prefix_")).isFalse();
        assertThat(selector.select("prefix_2")).isFalse();
    }

    @NonNull
    private static EnvelopeEncryption.TopicSelectorConfig getConfig(List<String> includeNames, List<String> excludeNames, List<String> includeNamePrefixes,
                                                                    List<String> excludeNamePrefixes) {
        return new EnvelopeEncryption.TopicSelectorConfig(includeNames, excludeNames, includeNamePrefixes, excludeNamePrefixes);
    }

}