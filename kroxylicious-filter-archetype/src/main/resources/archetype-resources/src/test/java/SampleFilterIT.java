package ${package};

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This is a simple end-to-end test that demonstrates how Kroxylicious intercepts and modifies Apache Kafka
 * messages in-transit.
 *
 * <p>It validates the core message filtering functionality by performing the following steps:</p>
 * <ol>
 *     <li>Starts an in-VM Apache Kafka cluster. (Injected by {@link KafkaClusterExtension})</li>
 *     <li>Starts the Kroxylicious proxy, configured with the filters and the Apache Kafka
 *      cluster as the upstream target</li>
 *     <li>Sends and receives a message using the standard Apache Kafka clients connected through the proxy</li>
 *     <li>Asserts that the messages received by the consumer is different from the one originally sent,
 *      proving that the filter successfully mutated the message.</li>
 * </ol>
 */
@ExtendWith(KafkaClusterExtension.class)
class SampleFilterIT {

    private static final Map<String, Object> CONSUMER_CONFIGURATION =
            Map.of(ConsumerConfig.GROUP_ID_CONFIG, "group-id-0",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private static final Map<String, Object> FILTER_CONFIGURATION =
            Map.of("findValue", "foo", "replacementValue", "bar");

    @Test
    void produceRequestFilter_shouldFindAndReplaceConfiguredWordInProducedMessages(
            @BrokerCluster final KafkaCluster kafkaCluster, final Topic topic) {
        // configure the filters with the proxy
        final var filterDefinition = new NamedFilterDefinitionBuilder("find-and-replace-produce-filter",
                SampleProduceRequest.class.getName()).withConfig(FILTER_CONFIGURATION).build();
        final var proxyConfiguration = KroxyliciousConfigUtils.proxy(kafkaCluster);
        proxyConfiguration.addToFilterDefinitions(filterDefinition);
        proxyConfiguration.addToDefaultFilters(filterDefinition.name());
        // create proxy instance and a producer and a consumer connected to it
        try (final var tester = KroxyliciousTesters.kroxyliciousTester(proxyConfiguration);
                final var producer = tester.producer();
                final var consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), CONSUMER_CONFIGURATION)) {
            final ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic.name(), "This is foo!");
            assertThat(producer.send(producerRecord)).succeedsWithin(TIMEOUT);
            consumer.subscribe(List.of(topic.name()));
            assertThat(consumer.poll(TIMEOUT).records(topic.name()))
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .extracting(String::new)
                    .isEqualTo("This is bar!");
        }
    }

    @Test
    void fetchResponseFilter_shouldFindAndReplaceConfiguredWordInConsumedMessages(
            @BrokerCluster final KafkaCluster kafkaCluster, final Topic topic) {
        // configure the filters with the proxy
        final var filterDefinition = new NamedFilterDefinitionBuilder("find-and-replace-consume-filter",
                SampleFetchResponse.class.getName()).withConfig(FILTER_CONFIGURATION).build();
        final var proxyConfiguration = KroxyliciousConfigUtils.proxy(kafkaCluster);
        proxyConfiguration.addToFilterDefinitions(filterDefinition);
        proxyConfiguration.addToDefaultFilters(filterDefinition.name());
        // create proxy instance and a producer and a consumer connected to it
        try (final var tester = KroxyliciousTesters.kroxyliciousTester(proxyConfiguration);
                final var producer = tester.producer();
                final var consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), CONSUMER_CONFIGURATION)) {
            final ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topic.name(), "This is foo!");
            assertThat(producer.send(producerRecord)).succeedsWithin(TIMEOUT);
            consumer.subscribe(List.of(topic.name()));
            assertThat(consumer.poll(TIMEOUT).records(topic.name()))
                    .singleElement()
                    .extracting(ConsumerRecord::value)
                    .extracting(String::new)
                    .isEqualTo("This is bar!");
        }
    }
}
