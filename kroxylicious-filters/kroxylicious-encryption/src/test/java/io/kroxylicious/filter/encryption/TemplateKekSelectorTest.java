/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.Kms;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TemplateKekSelectorTest {

    @Test
    void shouldRejectUnknownPlaceholders() {
        var e = assertThrows(IllegalArgumentException.class, () -> getSelector(null, "foo-${topicId}-bar"));
        assertEquals("Unknown template parameter: topicId", e.getMessage());
    }

    @Test
    void shouldResolveWhenAliasExists() {
        // Given
        InMemoryKms kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var selector = getSelector(kms, "topic-${topicName}");

        var kek = kms.generateKey();
        kms.createAlias(kek, "topic-my-topic");

        // When
        final CompletionStage<Map<String, EncryptionScheme<UUID>>> actual = selector.selectKek(Set.of("my-topic"));

        // Then
        assertThat(actual).isCompletedWithValue(Map.of("my-topic", new EncryptionScheme<>(kek, EnumSet.of(RecordField.RECORD_VALUE))));
    }

    @Test
    void shouldResolveUnencryptedSchemeWhenAliasDoesNotExist() {
        InMemoryKms kms = UnitTestingKmsService.newInstance().buildKms(new UnitTestingKmsService.Config());
        var selector = getSelector(kms, "topic-${topicName}");

        // When
        final CompletionStage<Map<String, EncryptionScheme<UUID>>> actual = selector.selectKek(Set.of("my-topic"));

        // Then
        assertThat(actual).isCompletedWithValue(Map.of("my-topic", new EncryptionScheme<>(new UUID(0, 0), EnumSet.noneOf(RecordField.class))));
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelector(Kms<K, ?> kms, String template) {
        var config = new TemplateKekSelector.Config(template);
        return new TemplateKekSelector<K>().buildSelector(kms, config);
    }

}