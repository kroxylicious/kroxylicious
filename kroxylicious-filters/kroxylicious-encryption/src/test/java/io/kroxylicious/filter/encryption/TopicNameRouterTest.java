/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicNameRouterTest {

    @Test
    void defaultRoute() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("ab"));
        assertThat(routed).isEqualTo(Map.of(1L, Set.of("ab")));
    }

    @Test
    void exactNameRoute() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).addIncludeExactNameRoute("exact", 2L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("exact", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("exact"), 1L, Set.of("other")));
    }

    @Test
    void exactMultipleRoutesToSameDestination() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).addIncludeExactNameRoute("a", 2L).addIncludeExactNameRoute("b", 2L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "b", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "b"), 1L, Set.of("other")));
    }

    @Test
    void prefixRoute() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).addIncludePrefixRoute("ab", 2L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("ab", "abc", "a", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("ab", "abc"), 1L, Set.of("a", "other")));
    }

    @Test
    void excludePrefixRoute() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).addIncludePrefixRoute("a", 2L).addExcludePrefixRoute("abc").build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "ab"), 1L, Set.of("abc", "abcd", "other")));
    }

    @Test
    void excludeExactRoute() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).addIncludePrefixRoute("a", 2L).addExcludeExactNameRoute("abc").build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "ab", "abcd"), 1L, Set.of("abc", "other")));
    }

    @Test
    void aPreviouslyExcludedPrefixRouteCanBeReIncludedLater() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L)
                .addIncludePrefixRoute("a", 2L)
                .addExcludePrefixRoute("abc")
                .addIncludePrefixRoute("abc", 3L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "ab"), 1L, Set.of("other"), 3L, Set.of("abc", "abcd")));
    }

    @Test
    void aPreviouslyExcludedExactRouteCanBeReIncludedLater() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L)
                .addIncludePrefixRoute("a", 2L)
                .addExcludeExactNameRoute("abc")
                .addIncludeExactNameRoute("abc", 3L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "ab", "abcd"), 1L, Set.of("other"), 3L, Set.of("abc")));
    }

    @Test
    void aShorterPrefixIncludedAfterANameWasExcludedOverridesIt() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L)
                .addIncludePrefixRoute("a", 2L)
                .addExcludeExactNameRoute("abc")
                .addIncludePrefixRoute("ab", 3L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a"), 1L, Set.of("other"), 3L, Set.of("ab", "abc", "abcd")));
    }

    @Test
    void aShorterPrefixIncludedAfterALongerPrefixWasExcludedOverridesIt() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L)
                .addIncludePrefixRoute("a", 2L)
                .addExcludePrefixRoute("abcd")
                .addIncludePrefixRoute("abc", 3L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "abcde", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "ab"), 1L, Set.of("other"), 3L, Set.of("abc", "abcde", "abcd")));
    }

    /**
     * This is thinking of the case where we might have two configurations like:
     * 1. encrypt prefix abc_def using cipher ChaCha20
     * 2. encrypt prefix abc_ using cipher AES excluding prefix abc_def
     * It is a little redundant that a later configuration excludes abc_def, but we can unambiguously say
     * that abc_def should be handled by configuration 1.
     */
    @Test
    void aPreviouslyIncludedExactRouteBeatsAnExclusion() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L)
                .addIncludeExactNameRoute("abc", 3L)
                .addIncludePrefixRoute("a", 2L)
                .addExcludeExactNameRoute("abc").build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "abcd", "other"));
        assertThat(routed).isEqualTo(Map.of(2L, Set.of("a", "ab", "abcd"), 1L, Set.of("other"), 3L, Set.of("abc")));
    }

    @Test
    void testMoreSpecificPrefixBeatsLessSpecificPrefix() {
        TopicNameRouter<Long> router = TopicNameRouter.builder(1L).addIncludePrefixRoute("a", 2L).addIncludePrefixRoute("ab", 3L).build();
        Map<Long, Set<String>> routed = router.route(Set.of("ab", "abc", "a", "other"));
        assertThat(routed).isEqualTo(Map.of(1L, Set.of("other"), 2L, Set.of("a"), 3L, Set.of("ab", "abc")));
    }

    @Test
    void testSamePrefixCannotRouteToMultipleDestinations() {
        assertThatThrownBy(() -> {
            TopicNameRouter.builder(1L).addIncludePrefixRoute("a", 2L).addIncludePrefixRoute("ab", 3L).addIncludePrefixRoute("ab", 4L);
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testSameExactNameCannotRouteToMultipleDestinations() {
        assertThatThrownBy(() -> {
            TopicNameRouter.builder(1L).addIncludeExactNameRoute("a", 2L).addIncludeExactNameRoute("a", 3L);
        }).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void exactNameRouteBeatsPrefixRoute() {
        long defaultDestination = 1L;
        long exactNameDestination = 2L;
        long prefixDestination = 3L;
        TopicNameRouter<Long> router = TopicNameRouter.builder(defaultDestination)
                .addIncludeExactNameRoute("ab", exactNameDestination)
                .addIncludePrefixRoute("a", prefixDestination)
                .build();
        Map<Long, Set<String>> routed = router.route(Set.of("a", "ab", "abc", "other"));
        assertThat(routed).isEqualTo(Map.of(defaultDestination, Set.of("other"), exactNameDestination, Set.of("ab"), prefixDestination, Set.of("a", "abc")));
    }

    @Test
    void defaultRouteRequired() {
        assertThatThrownBy(() -> {
            TopicNameRouter.builder(null);
        }).isInstanceOf(NullPointerException.class);
    }

}