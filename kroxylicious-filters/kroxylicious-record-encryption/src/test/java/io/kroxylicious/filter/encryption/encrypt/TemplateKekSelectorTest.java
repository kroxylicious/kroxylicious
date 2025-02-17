/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.encrypt;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.filter.encryption.config.TemplateConfig;
import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.filter.encryption.config.TopicNameKekSelection;
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

    private UnitTestingKmsService kmsService;

    @BeforeEach
    void beforeEach() {
        kmsService = UnitTestingKmsService.newInstance();
    }

    @AfterEach
    public void afterEach() {
        Optional.ofNullable(kmsService).ifPresent(UnitTestingKmsService::close);
    }

    @Test
    void shouldRejectUnknownPlaceholders() {
        assertThatThrownBy(() -> getSelector(null, "foo-${topicId}-bar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown template parameter: topicId");
    }

    @Test
    void shouldResolveWhenAliasExists() {
        kmsService.initialize(new UnitTestingKmsService.Config());
        var kms = kmsService.buildKms();
        var selector = getSelector(kms, "topic-${topicName}");

        var kek = kms.generateKey();
        kms.createAlias(kek, "topic-my-topic");
        TopicNameKekSelection<UUID> selection = selector.selectKek(Set.of("my-topic")).toCompletableFuture().join();
        var map = selection.topicNameToKekId();
        assertThat(map)
                .hasSize(1)
                .containsEntry("my-topic", kek);
        assertThat(selection.unresolvedTopicNames()).isEmpty();
    }

    @Test
    void shouldNotThrowWhenAliasDoesNotExist() {
        kmsService.initialize(new UnitTestingKmsService.Config());
        var kms = kmsService.buildKms();
        var selector = getSelector(kms, "topic-${topicName}");

        TopicNameKekSelection<UUID> selection = selector.selectKek(Set.of("my-topic")).toCompletableFuture().join();
        var unresolvedTopicNames = selection.unresolvedTopicNames();
        assertThat(unresolvedTopicNames)
                .hasSize(1)
                .containsExactly("my-topic");

        assertThat(selection.topicNameToKekId()).isEmpty();
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
        var selector = getSelector(kms, "topic-${topicName}");
        TopicNameKekSelection<UUID> selection = selector.selectKek(Set.of("my-topic")).toCompletableFuture().get();
        var unresolvedTopicNames = selection.unresolvedTopicNames();
        assertThat(unresolvedTopicNames)
                .hasSize(1)
                .containsExactly("my-topic");

        assertThat(selection.topicNameToKekId()).isEmpty();
    }

    @Test
    void serviceExceptionsArePropagated() {
        var kms = mock(InMemoryKms.class);
        var result = CompletableFuture.<UUID> failedFuture(new KmsException("bang!"));
        when(kms.resolveAlias(anyString())).thenReturn(result);

        var selector = getSelector(kms, "topic-${topicName}");
        var stage = selector.selectKek(Set.of("my-topic"));
        assertThat(stage)
                .isCompletedExceptionally()
                .failsWithin(Duration.ZERO)
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class);
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelector(Kms<K, ?> kms, String template) {
        var config = new TemplateConfig(template);
        return new TemplateKekSelector<K>().buildSelector(kms, config);
    }

}
