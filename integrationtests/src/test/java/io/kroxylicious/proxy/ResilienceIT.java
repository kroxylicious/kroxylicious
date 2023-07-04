/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import io.kroxylicious.proxy.config.ClusterNetworkAddressConfigProviderDefinitionBuilder;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeytoolCertificateGenerator;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.withDefaultFilters;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
@ExtendWith(KafkaClusterExtension.class)
public class ResilienceIT {

    static @BrokerCluster(numBrokers = 3) KafkaCluster cluster;

    @TempDir
    private Path certsDirectory;

    public static final HostPort PROXY_ADDRESS = HostPort.parse("localhost:9192");

    @Test
    public void producerShouldToleratePortPerBrokerExposedKroxyliciousRestarting(Admin admin) throws Exception {
        String randomTopic = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(randomTopic, 1, (short) 1))).all().get();
        testProducerCanSurviveARestart(withDefaultFilters(proxy(cluster)), randomTopic);
    }

    @Test
    public void producerShouldTolerateSniExposedKroxyliciousRestarting(Admin admin) throws Exception {
        String randomTopic = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(randomTopic, 1, (short) 1))).all().get();
        testProducerCanSurviveARestart(sniConfiguration(cluster), randomTopic);
    }

    @Test
    public void consumerShouldToleratePortPerBrokerExposedKroxyliciousRestarting(Admin admin) throws Exception {
        String randomTopic = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(randomTopic, 1, (short) 1))).all().get();
        testConsumerCanSurviveKroxyliciousRestart(withDefaultFilters(proxy(cluster)), randomTopic);
    }

    @Test
    public void consumerShouldTolerateSniExposedKroxyliciousRestarting(Admin admin) throws Exception {
        String randomTopic = UUID.randomUUID().toString();
        admin.createTopics(List.of(new NewTopic(randomTopic, 1, (short) 1))).all().get();
        testConsumerCanSurviveKroxyliciousRestart(sniConfiguration(cluster), randomTopic);
    }

    private static void testConsumerCanSurviveKroxyliciousRestart(ConfigurationBuilder builder, String topic)
            throws InterruptedException, ExecutionException, TimeoutException {
        Producer<String, String> producer;
        Consumer<String, String> consumer;
        try (var tester = kroxyliciousTester(builder)) {
            producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000));
            consumer = tester.consumer();
            producer.send(new ProducerRecord<>(topic, "my-key", "Hello, world!")).get(10, TimeUnit.SECONDS);
            consumer.subscribe(Set.of(topic));
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            assertEquals("Hello, world!", records.iterator().next().value());
            producer.send(new ProducerRecord<>(topic, "my-key", "Hello, again!")).get(10, TimeUnit.SECONDS);
        }
        try (var ignored = kroxyliciousTester(builder)) {
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            assertEquals("Hello, again!", records.iterator().next().value());
        }
    }

    private static void testProducerCanSurviveARestart(ConfigurationBuilder builder, String topic) throws InterruptedException, ExecutionException, TimeoutException {
        Producer<String, String> producer;
        Consumer<String, String> consumer;
        try (var tester = kroxyliciousTester(builder)) {
            producer = tester.producer(Map.of(CLIENT_ID_CONFIG, "shouldPassThroughRecordUnchanged", DELIVERY_TIMEOUT_MS_CONFIG, 3_600_000, RECONNECT_BACKOFF_MS_CONFIG, 5,
                    RECONNECT_BACKOFF_MAX_MS_CONFIG, 100, RETRY_BACKOFF_MS_CONFIG, 0));
            consumer = tester.consumer();
            producer.send(new ProducerRecord<>(topic, "my-key", "Hello, world!")).get(10, TimeUnit.SECONDS);
        }
        try (var ignored = kroxyliciousTester(builder)) {
            producer.send(new ProducerRecord<>(topic, "my-key", "Hello, again!")).get(10, TimeUnit.SECONDS);
            consumer.subscribe(Set.of(topic));
            var records = consumer.poll(Duration.ofSeconds(10));
            consumer.close();
            assertEquals(2, records.count());
            Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            assertEquals("Hello, world!", iterator.next().value());
            assertEquals("Hello, again!", iterator.next().value());
        }
    }

    private ConfigurationBuilder sniConfiguration(KafkaCluster cluster) throws IOException, GeneralSecurityException {
        var brokerCertificateGenerator = new KeytoolCertificateGenerator();
        brokerCertificateGenerator.generateSelfSignedCertificateEntry("test@redhat.com", "localhost", "KI", "RedHat", null, null, "US");
        var clientTrustStore = certsDirectory.resolve("kafka.truststore.jks");
        brokerCertificateGenerator.generateTrustStore(brokerCertificateGenerator.getCertFilePath(), "client",
                clientTrustStore.toAbsolutePath().toString());

        String bootstrapServers = cluster.getBootstrapServers();

        var builder = new ConfigurationBuilder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers(bootstrapServers)
                        .endTargetCluster()
                        .withClusterNetworkAddressConfigProvider(
                                new ClusterNetworkAddressConfigProviderDefinitionBuilder("PortPerBroker").withConfig("bootstrapAddress", PROXY_ADDRESS)
                                        .build())
                        .build())
                .addToFilters(new FilterDefinitionBuilder("ApiVersions").build());

        var demo = builder.getVirtualClusters().get("demo");
        demo = new VirtualClusterBuilder(demo)
                .withNewTls()
                .withNewKeyStoreKey()
                .withStoreFile(brokerCertificateGenerator.getKeyStoreLocation())
                .withNewInlinePasswordStoreProvider(brokerCertificateGenerator.getPassword())
                .endKeyStoreKey()
                .endTls()
                .build();
        builder.addToVirtualClusters("demo", demo);
        return builder;
    }

}
