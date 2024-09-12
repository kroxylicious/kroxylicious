/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.encryption;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.filter.encryption.EnvelopeEncryption;
import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.kms.service.TestKmsFacade;
import io.kroxylicious.kms.service.TestKmsFacadeInvocationContextProvider;
import io.kroxylicious.proxy.config.FilterDefinition;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(TestKmsFacadeInvocationContextProvider.class)
class EnvelopeEncryptionFilterIT {

    private static final String TEMPLATE_KEK_SELECTOR_PATTERN = "${topicName}";
    private static final String HELLO_WORLD = "hello world";

    @TestTemplate
    void roundTripSingleRecord(KafkaCluster cluster, Topic topic, TestKmsFacade<?, ?, ?> testKmsFacade) throws Exception {
        var testKekManager = testKmsFacade.getTestKekManager();
        testKekManager.generateKek(topic.name());

        var builder = proxy(cluster);

        builder.addToFilters(buildEncryptionFilterDefinition(testKmsFacade));

        try (var tester = kroxyliciousTester(builder);
                var producer = tester.producer();
                var consumer = tester.consumer()) {

            producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD)).get(5, TimeUnit.SECONDS);

            consumer.subscribe(List.of(topic.name()));
            var records = consumer.poll(Duration.ofSeconds(2));
            assertThat(records.iterator())
                                          .toIterable()
                                          .singleElement()
                                          .extracting(ConsumerRecord::value)
                                          .isEqualTo(HELLO_WORLD);
        }
    }

    @SuppressWarnings("removal")
    private FilterDefinition buildEncryptionFilterDefinition(TestKmsFacade<?, ?, ?> testKmsFacade) {
        return new FilterDefinitionBuilder(EnvelopeEncryption.class.getSimpleName())
                                                                                    .withConfig("kms", testKmsFacade.getKmsServiceClass().getSimpleName())
                                                                                    .withConfig("kmsConfig", testKmsFacade.getKmsServiceConfig())
                                                                                    .withConfig("selector", TemplateKekSelector.class.getSimpleName())
                                                                                    .withConfig("selectorConfig", Map.of("template", TEMPLATE_KEK_SELECTOR_PATTERN))
                                                                                    .build();
    }
}
