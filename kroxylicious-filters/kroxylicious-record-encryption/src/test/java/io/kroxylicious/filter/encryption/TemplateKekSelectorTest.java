/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
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
    private InMemoryKms kms;

    @BeforeEach
    void beforeEach() {
        kmsService = UnitTestingKmsService.newInstance();
        kmsService.initialize(new UnitTestingKmsService.Config());
        kms = kmsService.buildKms();
    }

    @AfterEach
    public void afterEach() {
        Optional.ofNullable(kmsService).ifPresent(UnitTestingKmsService::close);
    }

    @Test
    void shouldRejectUnknownPlaceholders() {
        assertThatThrownBy(() -> getSelector(kms, "foo-${topicId}-bar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown template parameter: topicId");
    }

    @Test
    void shouldResolveWhenAliasExists() {
        var selector = getSelector(kms, "topic-${topicName}");

        var kek = kms.generateKey();
        kms.createAlias(kek, "topic-my-topic");
        assertThat(selector.selectKek(Set.of("my-topic")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("my-topic", kek);
    }

    @Test
    void supportTemplateWithoutTopicPlaceholders() {
        var selector = getSelector(kms, "fixed");

        var kek = kms.generateKey();
        kms.createAlias(kek, "fixed");
        assertThat(selector.selectKek(Set.of("my-topic")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("my-topic", kek);
    }

    @Test
    void shouldNotThrowWhenAliasDoesNotExist() {
        var selector = getSelector(kms, "topic-${topicName}");

        assertThat(selector.selectKek(Set.of("my-topic")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("my-topic", null);
    }

    @Test
    void shouldNotThrowWhenAliasDoesNotExist_UnknownAliasExceptionWrappedInCompletionException() throws ExecutionException, InterruptedException {
        var mockKms = mock(InMemoryKms.class);
        var result = CompletableFuture.completedFuture(null)
                .<UUID> thenApply(u -> {
                    // this exception will be wrapped by a CompletionException
                    throw new UnknownAliasException("mock alias exception");
                });
        when(mockKms.resolveAlias(anyString())).thenReturn(result);
        var selector = getSelector(mockKms, "topic-${topicName}");
        assertThat(selector.selectKek(Set.of("my-topic")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("my-topic", null);
    }

    @Test
    void serviceExceptionsArePropagated() {
        var mockKms = mock(InMemoryKms.class);
        var result = CompletableFuture.<UUID> failedFuture(new KmsException("bang!"));
        when(mockKms.resolveAlias(anyString())).thenReturn(result);

        var selector = getSelector(mockKms, "topic-${topicName}");
        assertThat(selector.selectKek(Set.of("my-topic")))
                .isCompletedExceptionally()
                .failsWithin(Duration.ZERO)
                .withThrowableThat()
                .withCauseInstanceOf(KmsException.class);
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelector(Kms<K, ?> kms, String template) {
        var config = new TemplateKekSelector.TemplateConfig(template);
        return new TemplateKekSelector<K>().buildSelector(kms, config);
    }

}
