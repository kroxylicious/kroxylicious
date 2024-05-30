/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metadata.handler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.config.TopicLabelling;
import io.kroxylicious.proxy.metadata.selector.Selector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StaticTopicMetadataSourceTest {

    static List<Arguments> constructorRejectsOverlappingLabellings() {
        return List.of(
                Arguments.of(
                        new TopicLabelling(
                                Map.of("foo", "x"),
                                List.of("abc", "xyz"),
                                List.of(),
                                List.of()),
                        new TopicLabelling(
                                Map.of("foo", "y"),
                                List.of("xyz"), // xyz is a common topic
                                List.of(),
                                List.of())),
                Arguments.of(
                        new TopicLabelling(
                                Map.of("foo", "x"),
                                List.of("abc"),
                                List.of(),
                                List.of()),
                        new TopicLabelling(
                                Map.of("foo", "y"),
                                List.of(),
                                List.of("abc"), // abc is a common topic
                                List.of())),
                Arguments.of(
                        new TopicLabelling(
                                Map.of("foo", "x"),
                                List.of(),
                                List.of("abc1"), // Any topic starting with abc1 would be a common topic
                                List.of()),
                        new TopicLabelling(
                                Map.of("foo", "y"),
                                List.of(),
                                List.of("abc"),
                                List.of())),
                Arguments.of(
                        // Sadly although these are actually disjoint, our algorithm isn't able to prove it
                        new TopicLabelling(
                                Map.of("foo", "x"),
                                List.of("abc"),
                                List.of(),
                                List.of()),
                        new TopicLabelling(
                                Map.of("foo", "y"),
                                List.of(),
                                List.of(),
                                List.of("xyz"))));
    }

    @ParameterizedTest
    @MethodSource
    void constructorRejectsOverlappingLabellings(TopicLabelling l1, TopicLabelling l2) {
        List<TopicLabelling> labellings = List.of(l1, l2);
        assertThatThrownBy(() -> new StaticTopicMetadataSource(labellings))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("A topic cannot be labelled with both foo=x and foo=y: "
                        + l1
                        + " could have a topic in common with "
                        + l2);
    }

    static List<Arguments> constructorAcceptsNonoverlappingLabellings() {
        return Stream.concat(Stream.concat(
                // Take the pairs which are not disjoint and modify the labels so that they are disjoint
                constructorRejectsOverlappingLabellings().stream()
                        .map(args -> {
                            TopicLabelling l1 = (TopicLabelling) args.get()[0];
                            TopicLabelling l2 = (TopicLabelling) args.get()[1];
                            return Arguments.of(
                                    new TopicLabelling(Map.of("foo", "x"), l1.topicsNamed(), l1.topicsStartingWith(), l1.topicsMatching()),
                                    new TopicLabelling(Map.of("bar", "y"), l2.topicsNamed(), l2.topicsStartingWith(), l2.topicsMatching()));
                        }),
                constructorRejectsOverlappingLabellings().stream()
                        .map(args -> {
                            TopicLabelling l1 = (TopicLabelling) args.get()[0];
                            TopicLabelling l2 = (TopicLabelling) args.get()[1];
                            return Arguments.of(
                                    new TopicLabelling(Map.of("foo", "x"), l1.topicsNamed(), l1.topicsStartingWith(), l1.topicsMatching()),
                                    new TopicLabelling(Map.of("foo", "x"), l2.topicsNamed(), l2.topicsStartingWith(), l2.topicsMatching()));
                        })),
                // Also add some which have conflicting labels, but provably disjoint topics
                Stream.of(
                        Arguments.of(
                                new TopicLabelling(
                                        Map.of("foo", "x"),
                                        List.of("abc", "xyz"),
                                        List.of(),
                                        List.of()),
                                new TopicLabelling(
                                        Map.of("foo", "y"),
                                        List.of("pqr"),
                                        List.of(),
                                        List.of())),
                        Arguments.of(
                                new TopicLabelling(
                                        Map.of("foo", "x"),
                                        List.of("abc"),
                                        List.of(),
                                        List.of()),
                                new TopicLabelling(
                                        Map.of("foo", "y"),
                                        List.of(),
                                        List.of("abc1"),
                                        List.of())),
                        Arguments.of(
                                new TopicLabelling(
                                        Map.of("foo", "x"),
                                        List.of(),
                                        List.of("abc1"),
                                        List.of()),
                                new TopicLabelling(
                                        Map.of("foo", "y"),
                                        List.of(),
                                        List.of("abc2"),
                                        List.of()))))
                .toList();
    }

    @ParameterizedTest
    @MethodSource
    void constructorAcceptsNonoverlappingLabellings(TopicLabelling l1, TopicLabelling l2) {
        List<TopicLabelling> labellings = List.of(l1, l2);
        new StaticTopicMetadataSource(labellings);
    }

    @Test
    void topicLabels() {
        var stms = new StaticTopicMetadataSource(List.of(
                new TopicLabelling(
                        Map.of("foo", "x"),
                        List.of("abc", "xyz"),
                        List.of(),
                        List.of()),
                new TopicLabelling(
                        Map.of("foo", "y"),
                        List.of(),
                        List.of("pqr"),
                        List.of())));
        assertThat(topicLabels(stms, "abc")).isEqualTo(Map.of("foo", "x"));
        assertThat(topicLabels(stms, "xyz")).isEqualTo(Map.of("foo", "x"));
        assertThat(topicLabels(stms, "pqr")).isEqualTo(Map.of("foo", "y"));
        assertThat(topicLabels(stms, "pqr1")).isEqualTo(Map.of("foo", "y"));
        assertThat(topicLabels(stms, "def")).isEqualTo(Map.of());
    }

    private static Map<String, String> topicLabels(StaticTopicMetadataSource stms, String topicName) {
        return stms.topicLabels(Set.of(topicName))
                .toCompletableFuture()
                .join()
                .topicLabels(topicName);
    }

    @Test
    void topicsMatchingEquality() {
        var stms = new StaticTopicMetadataSource(List.of(
                new TopicLabelling(
                        Map.of("foo", "x"),
                        List.of("abc", "xyz"),
                        List.of(),
                        List.of()),
                new TopicLabelling(
                        Map.of("foo", "y"),
                        List.of(),
                        List.of("pqr"),
                        List.of())));
        assertThat(topicsMatching(stms, Set.of("abc", "xyz", "pqr", "pqr1", "unknown"), Selector.parse("foo=x"))).isEqualTo(Set.of("abc", "xyz"));
        assertThat(topicsMatching(stms, Set.of("abc", "xyz", "pqr", "pqr1", "unknown"), Selector.parse("foo=y"))).isEqualTo(Set.of("pqr", "pqr1"));
        assertThat(topicsMatching(stms, Set.of("abc", "xyz", "pqr", "pqr1", "unknown"), Selector.parse("foo=z"))).isEqualTo(Set.of());

    }

    @Test
    void topicsMatchingInequality() {
        var stms = new StaticTopicMetadataSource(List.of(
                new TopicLabelling(
                        Map.of("foo", "x"),
                        List.of("abc", "xyz"),
                        List.of(),
                        List.of()),
                new TopicLabelling(
                        Map.of("foo", "y"),
                        List.of(),
                        List.of("pqr"),
                        List.of())));
        assertThat(topicsMatching(stms, Set.of("abc", "xyz", "pqr", "pqr1", "unknown"), Selector.parse("foo!=x"))).isEqualTo(Set.of("pqr", "pqr1", "unknown"));
        assertThat(topicsMatching(stms, Set.of("abc", "xyz", "pqr", "pqr1", "unknown"), Selector.parse("foo!=y"))).isEqualTo(Set.of("abc", "xyz", "unknown"));
        assertThat(topicsMatching(stms, Set.of("abc", "xyz", "pqr", "pqr1", "unknown"), Selector.parse("foo!=z")))
                .isEqualTo(Set.of("abc", "xyz", "pqr", "pqr1", "unknown"));

    }

    private static Set<String> topicsMatching(StaticTopicMetadataSource stms, Set<String> topicNames, Selector selector) {
        return stms.topicsMatching(topicNames, Set.of(selector))
                .toCompletableFuture()
                .join()
                .topicsMatching(selector);
    }
}