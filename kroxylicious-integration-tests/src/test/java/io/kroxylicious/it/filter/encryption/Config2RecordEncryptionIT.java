/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.encryption;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.encryption.RecordEncryption;
import io.kroxylicious.filter.encryption.TemplateKekSelector;
import io.kroxylicious.filter.encryption.config.KekSelectorService;
import io.kroxylicious.filter.encryption.config.UnresolvedKeyPolicy;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.InMemoryTestKmsFacade;
import io.kroxylicious.kms.provider.kroxylicious.inmemory.IntegrationTestingKmsService;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.test.tester.FilesystemSnapshotBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Config2 integration tests for the RecordEncryption filter, exercising the full
 * config2 parsing pipeline with multi-plugin dependency resolution (RecordEncryption
 * depends on KmsService and KekSelectorService plugin instances).
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class Config2RecordEncryptionIT {

    private static final String HELLO_WORLD = "hello world";
    private static final String HELLO_SECRET = "hello secret";

    @Test
    void roundTripSingleRecord(
                               @BrokerCluster KafkaCluster cluster,
                               Topic topic,
                               @TempDir Path configDir)
            throws Exception {
        var facade = new InMemoryTestKmsFacade();
        facade.start();
        try {
            facade.getTestKekManager().generateKek(topic.name());

            Configuration configuration = buildConfig2Configuration(
                    configDir, cluster.getBootstrapServers(),
                    facade.getKmsServiceConfig().name(),
                    UnresolvedKeyPolicy.REJECT);

            try (var tester = kroxyliciousTester(configuration);
                    var producer = tester.producer();
                    var consumer = tester.consumer()) {

                producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD))
                        .get(5, TimeUnit.SECONDS);

                consumer.subscribe(List.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(2));
                assertThat(records.iterator())
                        .toIterable()
                        .singleElement()
                        .extracting(ConsumerRecord::value)
                        .isEqualTo(HELLO_WORLD);
            }
        }
        finally {
            facade.stop();
        }
    }

    @Test
    void topicRecordsAreUnreadableOnServer(
                                           @BrokerCluster KafkaCluster cluster,
                                           KafkaConsumer<String, String> directConsumer,
                                           Topic topic,
                                           @TempDir Path configDir)
            throws Exception {
        var facade = new InMemoryTestKmsFacade();
        facade.start();
        try {
            facade.getTestKekManager().generateKek(topic.name());

            Configuration configuration = buildConfig2Configuration(
                    configDir, cluster.getBootstrapServers(),
                    facade.getKmsServiceConfig().name(),
                    UnresolvedKeyPolicy.REJECT);

            try (var tester = kroxyliciousTester(configuration);
                    var producer = tester.producer()) {

                producer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD))
                        .get(5, TimeUnit.SECONDS);

                var tps = List.of(new TopicPartition(topic.name(), 0));
                directConsumer.assign(tps);
                directConsumer.seekToBeginning(tps);
                var records = directConsumer.poll(Duration.ofSeconds(2));
                assertThat(records.iterator())
                        .toIterable()
                        .singleElement()
                        .extracting(ConsumerRecord::value)
                        .isNotEqualTo(HELLO_WORLD);
            }
        }
        finally {
            facade.stop();
        }
    }

    @Test
    void decryptionAfterKekRotation(
                                    @BrokerCluster KafkaCluster cluster,
                                    Topic topic,
                                    @TempDir Path configDir)
            throws Exception {
        var facade = new InMemoryTestKmsFacade();
        facade.start();
        try {
            facade.getTestKekManager().generateKek(topic.name());

            Configuration configuration = buildConfig2Configuration(
                    configDir, cluster.getBootstrapServers(),
                    facade.getKmsServiceConfig().name(),
                    UnresolvedKeyPolicy.REJECT);

            var messageBeforeRotation = "hello world, old key";
            var messageAfterRotation = "hello world, new key";

            try (var tester = kroxyliciousTester(configuration);
                    var producer = tester.producer()) {

                producer.send(new ProducerRecord<>(topic.name(), messageBeforeRotation))
                        .get(5, TimeUnit.SECONDS);

                facade.getTestKekManager().rotateKek(topic.name());

                producer.send(new ProducerRecord<>(topic.name(), messageAfterRotation))
                        .get(5, TimeUnit.SECONDS);

                try (var consumer = tester.consumer()) {
                    consumer.subscribe(List.of(topic.name()));
                    var records = consumer.poll(Duration.ofSeconds(2));
                    assertThat(records.iterator())
                            .toIterable()
                            .extracting(ConsumerRecord::value)
                            .containsExactly(messageBeforeRotation, messageAfterRotation);
                }
            }
        }
        finally {
            facade.stop();
        }
    }

    @Test
    void unencryptedRecordsConsumable(
                                      @BrokerCluster KafkaCluster cluster,
                                      KafkaProducer<String, String> directProducer,
                                      Topic topic,
                                      @TempDir Path configDir)
            throws Exception {
        var facade = new InMemoryTestKmsFacade();
        facade.start();
        try {
            facade.getTestKekManager().generateKek(topic.name());

            Configuration configuration = buildConfig2Configuration(
                    configDir, cluster.getBootstrapServers(),
                    facade.getKmsServiceConfig().name(),
                    UnresolvedKeyPolicy.REJECT);

            try (var tester = kroxyliciousTester(configuration);
                    var producer = tester.producer();
                    var consumer = tester.consumer()) {

                // messages produced via Kroxylicious will be encrypted
                producer.send(new ProducerRecord<>(topic.name(), HELLO_SECRET))
                        .get(5, TimeUnit.SECONDS);

                // messages produced directly will be plain
                directProducer.send(new ProducerRecord<>(topic.name(), HELLO_WORLD))
                        .get(5, TimeUnit.SECONDS);

                consumer.subscribe(List.of(topic.name()));
                var records = consumer.poll(Duration.ofSeconds(2));
                assertThat(records.iterator())
                        .toIterable()
                        .hasSize(2)
                        .map(ConsumerRecord::value)
                        .contains(HELLO_SECRET, HELLO_WORLD);
            }
        }
        finally {
            facade.stop();
        }
    }

    @Test
    void userCanChooseToRejectRecords(
                                      @BrokerCluster KafkaCluster cluster,
                                      Topic encryptedTopic,
                                      Topic plainTopic,
                                      @TempDir Path configDir) {
        var facade = new InMemoryTestKmsFacade();
        facade.start();
        try {
            facade.getTestKekManager().generateKek(encryptedTopic.name());

            Configuration configuration = buildConfig2Configuration(
                    configDir, cluster.getBootstrapServers(),
                    facade.getKmsServiceConfig().name(),
                    UnresolvedKeyPolicy.REJECT);

            try (var tester = kroxyliciousTester(configuration);
                    var producer = tester.producer(
                            Map.of(ProducerConfig.RETRIES_CONFIG, 0,
                                    ProducerConfig.LINGER_MS_CONFIG, 1000))) {
                Future<RecordMetadata> sendA = producer.send(
                        new ProducerRecord<>(encryptedTopic.name(), HELLO_SECRET));
                Future<RecordMetadata> sendB = producer.send(
                        new ProducerRecord<>(plainTopic.name(), HELLO_WORLD));
                producer.flush();
                assertThat(sendA)
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableThat()
                        .withCause(new InvalidRecordException(
                                "failed to resolve key for: [" + plainTopic.name() + "]"));
                assertThat(sendB)
                        .failsWithin(5, TimeUnit.SECONDS)
                        .withThrowableThat()
                        .withCause(new InvalidRecordException(
                                "failed to resolve key for: [" + plainTopic.name() + "]"));
            }
        }
        finally {
            facade.stop();
        }
    }

    private Configuration buildConfig2Configuration(
                                                    Path configDir,
                                                    String bootstrapServers,
                                                    String kmsName,
                                                    UnresolvedKeyPolicy unresolvedKeyPolicy) {

        String filterYaml = """
                name: record-encryption
                type: %s
                version: v1
                config:
                  kms: my-kms
                  selector: my-selector
                  unresolvedKeyPolicy: %s
                """.formatted(
                RecordEncryption.class.getName(),
                unresolvedKeyPolicy.name());

        String kmsYaml = """
                name: my-kms
                type: %s
                version: v1
                config:
                  name: "%s"
                """.formatted(
                IntegrationTestingKmsService.class.getName(),
                kmsName);

        String selectorYaml = """
                name: my-selector
                type: %s
                version: v1
                config:
                  template: "$(topicName)"
                """.formatted(TemplateKekSelector.class.getName());

        return new FilesystemSnapshotBuilder(configDir)
                .proxyConfig(bootstrapServers, "record-encryption")
                .addPluginInstance(FilterFactory.class,
                        "record-encryption", filterYaml)
                .addPluginInstance(KmsService.class.getName(),
                        "my-kms", kmsYaml)
                .addPluginInstance(KekSelectorService.class.getName(),
                        "my-selector", selectorYaml)
                .buildConfiguration();
    }

}
