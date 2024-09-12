/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KroxyliciousClientsTest {

    private KroxyliciousClients kroxyliciousClients;

    @BeforeEach
    void setUp() {
        kroxyliciousClients = new KroxyliciousClients(
                Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kroxylicious.example.com:9091"),
                new KroxyliciousClients.ClientFactory() {
                    @Override
                    public Admin newAdmin(Map<String, Object> clientConfiguration) {
                        assertThat(clientConfiguration).contains(Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kroxylicious.example.com:9091"));
                        return mock(Admin.class);
                    }

                    @Override
                    public <K, V> Consumer<K, V> newConsumer(
                            Map<String, Object> clientConfiguration,
                            Deserializer<K> keyDeserializer,
                            Deserializer<V> valueDeserializer
                    ) {
                        assertThat(clientConfiguration).contains(Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kroxylicious.example.com:9091"));
                        return new MockConsumer<>(OffsetResetStrategy.LATEST);
                    }

                    @Override
                    public <K, V> Producer<K, V> newProducer(Map<String, Object> clientConfiguration, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
                        assertThat(clientConfiguration).contains(Map.entry(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "kroxylicious.example.com:9091"));
                        return new MockProducer<>();
                    }
                }
        );
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldCreateConsumer(Supplier<Consumer<String, String>> consumerSupplier) {
        // Given

        // When
        final Consumer<String, String> consumer = consumerSupplier.get();

        // Then
        assertThat(consumer).isInstanceOf(Consumer.class).isNotNull();
    }

    @ParameterizedTest
    @MethodSource("producers")
    void shouldCreateProducer(Supplier<Producer<String, String>> producerSupplier) {
        // Given

        // When
        final Producer<String, String> producer = producerSupplier.get();

        // Then
        assertThat(producer).isInstanceOf(Producer.class).isNotNull();
    }

    @Test
    void shouldCreateAdmin() {
        // Given

        // When
        final Admin admin = kroxyliciousClients.admin();

        // Then
        assertThat(admin).isInstanceOf(Admin.class).isNotNull();
    }

    @Test
    void shouldCloseAdminClient() {
        // Given
        final Admin admin = kroxyliciousClients.admin();

        // When
        kroxyliciousClients.close();

        // Then
        verify(admin).close();
    }

    @ParameterizedTest
    @MethodSource("producers")
    void shouldCloseProducer(Supplier<Producer<String, String>> producerSupplier) {
        // Given
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic", "Value");
        final Producer<String, String> producer = producerSupplier.get();

        // When
        kroxyliciousClients.close();

        // Then
        assertThatThrownBy(() -> producer.send(record))
                                                       .isInstanceOf(IllegalStateException.class)
                                                       .hasMessage("MockProducer is already closed.");
    }

    @ParameterizedTest
    @MethodSource("consumers")
    void shouldCloseConsumer(Supplier<Consumer<String, String>> consumerSupplier) {
        // Given
        final Consumer<String, String> consumer = consumerSupplier.get();

        // When
        kroxyliciousClients.close();

        // Then
        assertThatThrownBy(() -> consumer.poll(Duration.ZERO))
                                                              .isInstanceOf(IllegalStateException.class)
                                                              .hasMessage("This consumer has already been closed.");
    }

    @Test
    void shouldCloseMultipleProducers() {
        // Given
        final ProducerRecord<String, String> record = new ProducerRecord<>("topic", "Value");
        final Set<Producer<String, String>> clients = Set.of(kroxyliciousClients.producer(), kroxyliciousClients.producer(), kroxyliciousClients.producer());

        // When
        kroxyliciousClients.close();

        // Then
        for (Producer<String, String> producer : clients) {
            assertThatThrownBy(() -> producer.send(record))
                                                           .isInstanceOf(IllegalStateException.class)
                                                           .hasMessage("MockProducer is already closed.");
        }
    }

    @Test
    void shouldCloseMultipleConsumers() {
        // Given
        final Set<Consumer<String, String>> clients = Set.of(kroxyliciousClients.consumer(), kroxyliciousClients.consumer(), kroxyliciousClients.consumer());

        // When
        kroxyliciousClients.close();

        // Then
        for (Consumer<String, String> consumer : clients) {
            assertThatThrownBy(() -> consumer.poll(Duration.ZERO))
                                                                  .isInstanceOf(IllegalStateException.class)
                                                                  .hasMessage("This consumer has already been closed.");
        }
    }

    @Test
    void shouldCloseMultipleAdmins() {
        // Given
        final Set<Admin> clients = Set.of(kroxyliciousClients.admin(), kroxyliciousClients.admin(), kroxyliciousClients.admin());

        // When
        kroxyliciousClients.close();

        // Then
        for (Admin admin : clients) {
            verify(admin).close();
        }
    }

    public Stream<Arguments> producers() {
        return Stream.of(
                Arguments.of(
                        (Supplier<Producer<String, String>>) () -> kroxyliciousClients.producer(),
                        (Supplier<Producer<String, String>>) () -> kroxyliciousClients.producer(Map.of("configKey", "configValue")),
                        (Supplier<Producer<String, String>>) () -> kroxyliciousClients.producer(
                                Serdes.String(),
                                Serdes.String(),
                                Map.of("configKey", "configValue")
                        )
                )
        );
    }

    public Stream<Arguments> consumers() {
        return Stream.of(
                Arguments.of(
                        (Supplier<Consumer<String, String>>) () -> kroxyliciousClients.consumer(),
                        (Supplier<Consumer<String, String>>) () -> kroxyliciousClients.consumer(Map.of("configKey", "configValue")),
                        (Supplier<Consumer<String, String>>) () -> kroxyliciousClients.consumer(
                                Serdes.String(),
                                Serdes.String(),
                                Map.of("configKey", "configValue")
                        )
                )
        );
    }
}
