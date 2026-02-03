/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.filter.encryption.common.EncryptionException;
import io.kroxylicious.filter.encryption.config.RegexTopicNameConfig;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.UnknownAliasException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RegexTopicNameKekSelectorTest {

    @Mock
    Kms<String, String> kms;

    @Test
    public void testPassthroughAll() {
        RegexTopicNameConfig.Rule passthroughAllRule = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.PASSTHROUGH_UNENCRYPTED, null, List.of(".*"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(passthroughAllRule)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a", "b"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).containsExactly("a", "b");
            assertThat(selection.topicNameToKekId()).isEmpty();
            assertThat(selection.unresolvedTopicNames()).isEmpty();
        });
    }

    @Test
    public void noMatchingRule() {
        RegexTopicNameConfig.Rule passthroughOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.PASSTHROUGH_UNENCRYPTED, null, List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(passthroughOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("b"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).isEmpty();
            assertThat(selection.topicNameToKekId()).isEmpty();
            assertThat(selection.unresolvedTopicNames()).containsExactly("b");
        });
    }

    @Test
    public void somePassthroughSomeHaveNoRule() {
        RegexTopicNameConfig.Rule passthroughOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.PASSTHROUGH_UNENCRYPTED, null, List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(passthroughOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a", "b"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).containsExactly("a");
            assertThat(selection.topicNameToKekId()).isEmpty();
            assertThat(selection.unresolvedTopicNames()).containsExactly("b");
        });
    }

    // two rules are matched, lowest-index rule wins
    @Test
    public void firstMatchWins() {
        RegexTopicNameConfig.Rule passthroughOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.PASSTHROUGH_UNENCRYPTED, null, List.of("a"));
        RegexTopicNameConfig.Rule encryptOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.ENCRYPT, "kek", List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(passthroughOnlyA, encryptOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).containsExactly("a");
            assertThat(selection.topicNameToKekId()).isEmpty();
            assertThat(selection.unresolvedTopicNames()).isEmpty();
        });
    }

    @Test
    public void kekResolves() {
        when(kms.resolveAlias("a-kek")).thenReturn(CompletableFuture.completedFuture("a-kek-key"));
        RegexTopicNameConfig.Rule encryptOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.ENCRYPT, "a-kek", List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(encryptOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).isEmpty();
            assertThat(selection.topicNameToKekId()).containsExactlyEntriesOf(Map.of("a", "a-kek-key"));
            assertThat(selection.unresolvedTopicNames()).isEmpty();
        });
    }

    @Test
    public void kekAliasNotResolved() {
        when(kms.resolveAlias("a-kek")).thenReturn(CompletableFuture.failedFuture(new UnknownAliasException("unknown alias")));
        RegexTopicNameConfig.Rule encryptOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.ENCRYPT, "a-kek", List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(encryptOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).isEmpty();
            assertThat(selection.topicNameToKekId()).isEmpty();
            assertThat(selection.unresolvedTopicNames()).containsExactly("a");
        });
    }

    @Test
    public void kekAliasOtherExceptionPropagated() {
        EncryptionException cause = new EncryptionException("BOOM!");
        when(kms.resolveAlias("a-kek")).thenReturn(CompletableFuture.failedFuture(cause));
        RegexTopicNameConfig.Rule encryptOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.ENCRYPT, "a-kek", List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(encryptOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a"));
        assertThat(stage).failsWithin(Duration.ZERO).withThrowableThat().isInstanceOf(ExecutionException.class).havingCause().isEqualTo(cause);
    }

    @Test
    public void kekTemplateResolves() {
        when(kms.resolveAlias("a-kek")).thenReturn(CompletableFuture.completedFuture("a-kek-key"));
        RegexTopicNameConfig.Rule encryptOnlyA = new RegexTopicNameConfig.Rule(RegexTopicNameConfig.Behaviour.ENCRYPT, "$(topicName)-kek", List.of("a"));
        RegexTopicNameKekSelector<String> selector = new RegexTopicNameKekSelector<>(kms,
                new RegexTopicNameConfig(List.of(encryptOnlyA)));
        CompletionStage<TopicNameKekSelection<String>> stage = selector.selectKek(Set.of("a"));
        assertThat(stage).succeedsWithin(Duration.ZERO).satisfies(selection -> {
            assertThat(selection.passthroughTopicNames()).isEmpty();
            assertThat(selection.topicNameToKekId()).containsExactlyEntriesOf(Map.of("a", "a-kek-key"));
            assertThat(selection.unresolvedTopicNames()).isEmpty();
        });
    }

}