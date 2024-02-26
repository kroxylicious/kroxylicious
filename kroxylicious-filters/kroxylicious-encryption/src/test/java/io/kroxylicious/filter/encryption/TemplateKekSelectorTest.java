/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsException;
import io.kroxylicious.kms.service.UnknownAliasException;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TemplateKekSelectorTest {

    @Test
    void shouldRejectUnknownPlaceholders() {
        assertThatThrownBy(() -> getSelectorAny(null, "foo-${topicId}-bar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown template parameter: topicId");
    }

    @Test
    void shouldResolveWhenAliasExists() {
        var kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var selector = getSelectorExact(kms, "my-topic", "topic-${topicName}");

        var kek = kms.generateKey();
        kms.createAlias(kek, "topic-my-topic");
        var map = selector.selectKek(Set.of("my-topic")).toCompletableFuture().join();
        assertThat(map)
                .hasSize(1)
                .containsEntry("my-topic", kek);
    }

    @Test
    void shouldResolveWhenAliasExistsViaPrefix() {
        var kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var selector = getSelector(kms, List.of(new TemplateKekSelector.TopicNameMatcher(null, "my-topic", "key-for-${topicName}")));

        var kek1 = kms.generateKey();
        kms.createAlias(kek1, "key-for-my-topic");
        var kek2 = kms.generateKey();
        kms.createAlias(kek2, "key-for-my-topic-foo");
        kms.createAlias(kek2, "key-for-my-topic-bar");
        kms.createAlias(kek2, "key-for-my-topic-");
        kms.createAlias(kek2, "key-for-my-topic.");
        kms.createAlias(kek2, "key-for-my-topic_");
        kms.createAlias(kek2, "key-for-my-topic0");

        Map<String, UUID> expect = new HashMap<>();
        expect.put("my-topic", kek1); // exact match
        expect.put("my-topic-foo", kek2); // prefixed by topic name matcher prefix
        expect.put("my-topic-bar", kek2); // prefixed by topic name matcher prefix
        expect.put("my-topic-", kek2); // prefixed by topic name matcher prefix
        expect.put("my-topic.", kek2); // prefixed by topic name matcher prefix
        expect.put("my-topic_", kek2); // prefixed by topic name matcher prefix
        expect.put("my-topic0", kek2); // prefixed by topic name matcher prefix
        expect.put("my-topib", null); // precedes topic name matcher prefix
        expect.put("my-topibZZZZZZZZZZZZ", null); // precedes topic name matcher prefix
        expect.put("my-topid", null); // succeeds topic name matcher prefix

        assertThat(selector.selectKek(expect.keySet()).toCompletableFuture().join())
                .isEqualTo(expect);
    }

    @Test
    void shouldThrowWhenAliasDoesNotExist() {
        var kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var selector = getSelectorAny(kms, "topic-${topicName}");

        assertThat(selector.selectKek(Set.of("my-topic")))
                .failsWithin(Duration.ZERO)
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class);
    }

    @Test
    void shouldNotThrowWhenAliasDoesNotExist_UnknownAliasExceptionWrappedInCompletionException() throws ExecutionException, InterruptedException {
        var kms = mock(InMemoryKms.class);
        var result = CompletableFuture.completedFuture(null)
                .<UUID> thenApply((u) -> {
                    // this exception will be wrapped by a CompletionException
                    throw new UnknownAliasException("mock alias exception");
                });
        when(kms.resolveAlias(anyString())).thenReturn(result);
        var selector = getSelectorAny(kms, "topic-${topicName}");
        assertThat(selector.selectKek(Set.of("my-topic")))
                .failsWithin(Duration.ZERO)
                .withThrowableThat()
                .withCauseInstanceOf(UnknownAliasException.class);
    }

    @Test
    void serviceExceptionsArePropagated() {
        var kms = mock(InMemoryKms.class);
        var result = CompletableFuture.<UUID> failedFuture(new KmsException("bang!"));
        when(kms.resolveAlias(anyString())).thenReturn(result);

        var selector = getSelectorAny(kms, "topic-${topicName}");
        var stage = selector.selectKek(Set.of("my-topic"));
        assertThat(stage)
                .isCompletedExceptionally()
                .failsWithin(Duration.ZERO)
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class);
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelectorAny(Kms<K, ?> kms, String template) {
        List<TemplateKekSelector.TopicNameMatcher> templates = List.of(new TemplateKekSelector.TopicNameMatcher(null, "", template));
        return getSelector(kms, templates);
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelectorExact(Kms<K, ?> kms, String topicName, String template) {
        List<TemplateKekSelector.TopicNameMatcher> templates = List.of(new TemplateKekSelector.TopicNameMatcher(null, "", template));
        return getSelector(kms, templates);
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelector(Kms<K, ?> kms, List<TemplateKekSelector.TopicNameMatcher> templates) {
        var config = new TemplateKekSelector.Config(templates);
        return new TemplateKekSelector<K>().buildSelector(kms, config);
    }

}
