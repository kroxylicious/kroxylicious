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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.tester.KroxyliciousConfigUtils;
import io.kroxylicious.test.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SampleFilterIT {

    private static final Map<String, Object> CONSUMER_CONFIGURATION =
            Map.of(ConsumerConfig.GROUP_ID_CONFIG, "group-id-0",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    private static final int TIMEOUT = 10;

    private static final Map<String, Object> FILTER_CONFIGURATION =
            Map.of("findValue", "foo", "replacementValue", "bar");

    @BrokerCluster
    private KafkaCluster kafkaCluster;

    @SuppressWarnings("unused")
    private Topic topic;

    private ConfigurationBuilder proxyConfiguration;

    @BeforeEach
    public void setUp() {
        this.proxyConfiguration = KroxyliciousConfigUtils.proxy(kafkaCluster);
    }

    @Test
    void produceRequestFilter_shouldFindAndReplaceConfiguredWordInProducedMessages() throws Exception {
        // configure the filters with the proxy
        final var filterDefinition = new NamedFilterDefinitionBuilder("find-and-replace-produce-filter",
                SampleProduceRequest.class.getName()).withConfig(FILTER_CONFIGURATION).build();
        proxyConfiguration.addToFilterDefinitions(filterDefinition);
        proxyConfiguration.addToDefaultFilters(filterDefinition.name());
        // create proxy instance and producer, consumer to the proxy
        try (final var tester = KroxyliciousTesters.kroxyliciousTester(proxyConfiguration);
                final var producer = tester.producer();
                final var consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), CONSUMER_CONFIGURATION)) {
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic.name(), "This goes foo!");
            producer.send(producerRecord).get(TIMEOUT, TimeUnit.SECONDS);
            consumer.subscribe(List.of(topic.name()));
            final var message = consume(consumer, topic.name());
            assertThat(message).isEqualTo("This goes bar!");
        }
    }

    @Test
    void fetchResponseFilter_shouldFindAndReplaceConfiguredWordInConsumedMessages() throws Exception {
        // configure the filters with the proxy
        final var filterDefinition = new NamedFilterDefinitionBuilder("find-and-replace-consume-filter",
                SampleFetchResponse.class.getName()).withConfig(FILTER_CONFIGURATION).build();
        proxyConfiguration.addToFilterDefinitions(filterDefinition);
        proxyConfiguration.addToDefaultFilters(filterDefinition.name());
        try (final var tester = KroxyliciousTesters.kroxyliciousTester(proxyConfiguration);
                final var producer = tester.producer();
                final var consumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), CONSUMER_CONFIGURATION)) {
            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic.name(), "This goes foo!");
            producer.send(producerRecord).get(TIMEOUT, TimeUnit.SECONDS);
            consumer.subscribe(List.of(topic.name()));
            final var message = consume(consumer, topic.name());
            assertThat(message).isEqualTo("This goes bar!");
        }
    }

    private static String consume(final Consumer<String, byte[]> consumer, final String topic) {
        final ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(TIMEOUT));
        assertThat(records.count()).isGreaterThan(0);
        final var record = records.records(topic).iterator().next();
        assertThat(record).isNotNull();
        return new String(record.value(), StandardCharsets.UTF_8);
    }

}
