/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.message.AddOffsetsToTxnRequestData;
import io.kroxylicious.kafka.common.message.EndTxnRequestData;
import io.kroxylicious.kafka.common.message.FindCoordinatorRequestData;
import io.kroxylicious.kafka.common.message.HeartbeatRequestData;
import io.kroxylicious.kafka.common.message.HeartbeatResponseData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;

import static org.assertj.core.api.Assertions.assertThat;

class SchemaDrivenMessagePopulatorTest {

    private ValidScalarStrategy validScalarStrategy;

    @BeforeEach
    void setUp() {
        validScalarStrategy = new ValidScalarStrategy(new Random(42));
    }

    @Test
    void populatesKafkaInstance() {
        // Given
        org.apache.kafka.common.message.SaslHandshakeRequestData instance = new org.apache.kafka.common.message.SaslHandshakeRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.mechanism()).isNotNull();
    }

    @Test
    void populatesKroxyliciousInstanceUsingKafkaSchema() {
        // Given
        SaslHandshakeRequestData instance = new SaslHandshakeRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.mechanism()).isNotNull();
    }

    @Test
    void populatesBytesField() {
        // Given
        SaslAuthenticateRequestData instance = new SaslAuthenticateRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.authBytes()).isNotEmpty();
    }

    @Test
    void populatesInt32Field() {
        // Given
        HeartbeatRequestData instance = new HeartbeatRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.groupId()).isNotNull();
        assertThat(instance.memberId()).isNotNull();
    }

    @Test
    void populatesInt16Field() {
        // Given
        HeartbeatResponseData instance = new HeartbeatResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.errorCode()).isNotZero();
    }

    @Test
    void populatesInt8Field() {
        // Given
        FindCoordinatorRequestData instance = new FindCoordinatorRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 1);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.key()).isNotNull();
        assertThat(instance.keyType()).isNotZero();
    }

    @Test
    void populatesInt64Field() {
        // Given
        AddOffsetsToTxnRequestData instance = new AddOffsetsToTxnRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.producerId()).isNotZero();
    }

    @Test
    void populatesBooleanField() {
        // Given
        EndTxnRequestData instance = new EndTxnRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.transactionalId()).isNotNull();
    }

    @Test
    void sameSeedProducesSameValues() {
        // Given
        SaslHandshakeRequestData first = new SaslHandshakeRequestData();
        SaslHandshakeRequestData second = new SaslHandshakeRequestData();
        SchemaDrivenMessagePopulator firstPopulator = new SchemaDrivenMessagePopulator(new ValidScalarStrategy(new Random(798)));
        SchemaDrivenMessagePopulator secondPopulator = new SchemaDrivenMessagePopulator(new ValidScalarStrategy(new Random(798)));
        firstPopulator.populate(first, (short) 0);

        // When
        secondPopulator.populate(second, (short) 0);

        // Then
        assertThat(second.mechanism()).isEqualTo(first.mechanism());
    }
}
