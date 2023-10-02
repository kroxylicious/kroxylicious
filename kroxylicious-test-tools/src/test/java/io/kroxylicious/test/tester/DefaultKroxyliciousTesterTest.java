/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import org.apache.kafka.clients.admin.Admin;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({ KafkaClusterExtension.class, MockitoExtension.class })
class DefaultKroxyliciousTesterTest {

    @Mock
    DefaultKroxyliciousTester.ClientFactory clientFactory;

    @Mock
    KroxyliciousClients kroxyliciousClients;

    @Mock
    Admin admin;

    private final MockProducer<String, String> producer = new MockProducer<>();

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    @BeforeEach
    void setUp() {
        when(clientFactory.build(anyString(), anyString())).thenReturn(kroxyliciousClients);
        when(kroxyliciousClients.admin()).thenReturn(admin);
        when(kroxyliciousClients.producer()).thenReturn(producer);
        when(kroxyliciousClients.consumer()).thenReturn(consumer);
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForVirtualCluster(KafkaCluster backingCluster) {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.admin("demo");

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForDefaultVirtualCluster(KafkaCluster backingCluster) {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.admin();

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForVirtualCluster(KafkaCluster backingCluster) {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.producer("demo");

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForDefaultVirtualCluster(KafkaCluster backingCluster) {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.producer();

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForVirtualCluster(KafkaCluster backingCluster) {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.consumer("demo");

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).consumer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForDefaultVirtualCluster(KafkaCluster backingCluster) {
        // Given
        try (var tester = buildTester(backingCluster)) {

            // When
            tester.consumer();

            // Then
            //In theory the bootstrap address is predicable but asserting it is  not part of this test
            verify(clientFactory).build(eq("demo"), anyString());
            verify(kroxyliciousClients).consumer();
        }
    }

    @NotNull
    private DefaultKroxyliciousTester buildTester(KafkaCluster backingCluster) {
        return new DefaultKroxyliciousTester(proxy(backingCluster),
                DefaultKroxyliciousTester::spawnProxy,
                clientFactory);
    }
}