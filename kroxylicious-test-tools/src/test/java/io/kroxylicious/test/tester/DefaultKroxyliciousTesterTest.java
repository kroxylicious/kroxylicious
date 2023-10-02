/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultKroxyliciousTesterTest {

    @Mock
    KafkaCluster backingCluster;

    @Mock
    DefaultKroxyliciousTester.ClientFactory clientFactory;

    @Mock(strictness = Mock.Strictness.LENIENT)
    KroxyliciousClients kroxyliciousClients;

    @Mock
    Admin admin;

    private final MockProducer<String, String> producer = new MockProducer<>();

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    @BeforeEach
    void setUp() {
        when(backingCluster.getBootstrapServers()).thenReturn("broker01.example.com:9090");
        when(clientFactory.build(anyString(), anyString())).thenReturn(kroxyliciousClients);
        when(kroxyliciousClients.admin()).thenReturn(admin);
        when(kroxyliciousClients.producer()).thenReturn(producer);
        when(kroxyliciousClients.consumer()).thenReturn(consumer);
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForVirtualCluster() {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.admin("demo");

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForDefaultVirtualCluster() {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.admin();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForVirtualCluster() {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.producer("demo");

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForDefaultVirtualCluster() {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.producer();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForVirtualCluster() {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.consumer("demo");

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).consumer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForDefaultVirtualCluster() {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.consumer();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).consumer();
        }
    }

    @Test
    void closingTesterShouldCloseClients() {
        // Given
        try (var tester = buildTester(backingCluster)) {
            tester.consumer();

            // When
            tester.close();

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(kroxyliciousClients).close();
        }
    }

    @NotNull
    private DefaultKroxyliciousTester buildTester(KafkaCluster backingCluster) {
        return new DefaultKroxyliciousTester(proxy(backingCluster),
                DefaultKroxyliciousTester::spawnProxy,
                clientFactory);
    }
}