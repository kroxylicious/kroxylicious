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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.filter.encryption.config.TopicNameBasedKekSelector;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryKms;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.UnitTestingKmsService;
import io.kroxylicious.kms.service.Kms;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicPrefixBasedKekSelectorTest {

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

    @ParameterizedTest
    @ValueSource(strings = { "foo-${topicName}", "foo-${topicId}" })
    void shouldRejectUnknownPlaceholders(String template) {
        assertThatThrownBy(() -> getSelector(kms, template, "-"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown template parameter:");
    }

    @ParameterizedTest
    @ValueSource(strings = { "" })
    void shouldRejectMissingSeparator(String parameter) {
        assertThatThrownBy(() -> getSelector(kms, "foo-${topicPrefix}", parameter))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Prefix separator cannot be empty");
    }

    @Test
    void shouldResolveWhenAliasExists() {
        var selector = getSelector(kms, "kek-${topicPrefix}", "-");

        var kek = kms.generateKey();
        kms.createAlias(kek, "kek-sales");
        assertThat(selector.selectKek(Set.of("sales-inventory")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("sales-inventory", kek);
    }

    @Test
    void supportTemplateWithoutTopicPlaceholders() {
        var selector = getSelector(kms, "kek", "-");

        var kek = kms.generateKey();
        kms.createAlias(kek, "kek");
        assertThat(selector.selectKek(Set.of("sales-inventory")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("sales-inventory", kek);
    }

    @Test
    void shouldNotThrowWhenAliasDoesNotExist() {
        var selector = getSelector(kms, "kek-${topicPrefix}", "-");

        assertThat(selector.selectKek(Set.of("sales-inventory")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("sales-inventory", null);
    }

    @Test
    void shouldNotThrowWhenTopicDoesNotContainPrefix() {
        var selector = getSelector(kms, "kek-${topicPrefix}", "-");

        assertThat(selector.selectKek(Set.of("sales")))
                .succeedsWithin(Duration.ofSeconds(1))
                .asInstanceOf(InstanceOfAssertFactories.map(String.class, UUID.class))
                .hasSize(1)
                .containsEntry("sales", null);
    }

    @NonNull
    private <K> TopicNameBasedKekSelector<K> getSelector(Kms<K, ?> kms, String template, String separator) {
        var config = new TopicPrefixBasedKekSelector.TemplateConfig(separator, template);
        return new TopicPrefixBasedKekSelector<K>().buildSelector(kms, config);
    }

}