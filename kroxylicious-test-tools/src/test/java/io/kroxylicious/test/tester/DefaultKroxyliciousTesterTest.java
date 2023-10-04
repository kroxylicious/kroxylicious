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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.DEFAULT_VIRTUAL_CLUSTER;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultKroxyliciousTesterTest {

    private static final String VIRTUAL_CLUSTER_A = "clusterA";
    private static final String VIRTUAL_CLUSTER_B = "clusterB";
    private static final String VIRTUAL_CLUSTER_C = "clusterC";
    private static final String EXCEPTION_MESSAGE = "KaBOOM!!";
    private static final String DEFAULT_CLUSTER = "demo";
    String backingCluster = "broker01.example.com:9090";

    @Mock(strictness = LENIENT)
    DefaultKroxyliciousTester.ClientFactory clientFactory;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClients;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClientsA;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClientsB;

    @Mock(strictness = LENIENT)
    KroxyliciousClients kroxyliciousClientsC;

    @Mock
    Admin admin;

    private final MockProducer<String, String> producer = new MockProducer<>();

    private final MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    @BeforeEach
    void setUp() {
        when(clientFactory.build(eq(DEFAULT_VIRTUAL_CLUSTER), anyString())).thenReturn(kroxyliciousClients);
        when(clientFactory.build(eq(VIRTUAL_CLUSTER_A), anyString())).thenReturn(kroxyliciousClientsA);
        when(clientFactory.build(eq(VIRTUAL_CLUSTER_B), anyString())).thenReturn(kroxyliciousClientsB);
        when(clientFactory.build(eq(VIRTUAL_CLUSTER_C), anyString())).thenReturn(kroxyliciousClientsC);
        when(kroxyliciousClients.admin()).thenReturn(admin);
        when(kroxyliciousClients.producer()).thenReturn(producer);
        when(kroxyliciousClients.consumer()).thenReturn(consumer);
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.admin(DEFAULT_CLUSTER);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(DEFAULT_CLUSTER), anyString());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateAdminForDefaultVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.admin();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(DEFAULT_CLUSTER), anyString());
            verify(kroxyliciousClients).admin();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.producer(DEFAULT_CLUSTER);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(DEFAULT_CLUSTER), anyString());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateProducerForDefaultVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.producer();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(DEFAULT_CLUSTER), anyString());
            verify(kroxyliciousClients).producer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.consumer(DEFAULT_CLUSTER);

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(DEFAULT_CLUSTER), anyString());
            verify(kroxyliciousClients).consumer();
        }
    }

    @SuppressWarnings("resource")
    @Test
    void shouldCreateConsumerForDefaultVirtualCluster() {
        // Given
        try (var tester = buildDefaultTester()) {

            // When
            tester.consumer();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(clientFactory).build(eq(DEFAULT_CLUSTER), anyString());
            verify(kroxyliciousClients).consumer();
        }
    }

    @Test
    void closingTesterShouldCloseClients() {
        // Given
        try (var tester = buildDefaultTester()) {
            tester.consumer();

            // When
            tester.close();

            // Then
            // In theory the bootstrap address is predicable but asserting it is not part of this test
            verify(kroxyliciousClients).close();
        }
    }

    @Test
    void shouldKeepClosingClientsWhenOneFails() {
        // Given
        try (var tester = buildTester()) {
            tester.admin(VIRTUAL_CLUSTER_A);
            tester.admin(VIRTUAL_CLUSTER_B);
            tester.admin(VIRTUAL_CLUSTER_C);
            // The doNothing is required so the try-with-resources block completes successfully
            doThrow(new IllegalStateException(EXCEPTION_MESSAGE)).doNothing().when(kroxyliciousClientsA).close();

            // When
            try {
                tester.close();
                fail("Expected tester to re-throw");
            }
            catch (RuntimeException re) {
                // not my problem
            }

            // Then
            verify(kroxyliciousClientsA).close();
            verify(kroxyliciousClientsB).close();
            verify(kroxyliciousClientsC).close();
        }
    }

    @Test
    void shouldPropagateExceptionsOnClose() {
        // Given
        try (var tester = buildTester()) {
            tester.admin(VIRTUAL_CLUSTER_A);
            // The doNothing is required so the try-with-resources block completes successfully
            doThrow(new IllegalStateException(EXCEPTION_MESSAGE)).doNothing().when(kroxyliciousClientsA).close();

            // When
            // Then
            assertThatThrownBy(tester::close)
                    .cause()
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(EXCEPTION_MESSAGE);
        }
    }

    @NonNull
    private DefaultKroxyliciousTester buildDefaultTester() {
        return new DefaultKroxyliciousTester(proxy(backingCluster),
                DefaultKroxyliciousTester::spawnProxy,
                clientFactory);
    }

    private DefaultKroxyliciousTester buildTester() {
        return new DefaultKroxyliciousTester(proxy(backingCluster, VIRTUAL_CLUSTER_A, VIRTUAL_CLUSTER_B, VIRTUAL_CLUSTER_C),
                DefaultKroxyliciousTester::spawnProxy,
                clientFactory);
    }
}