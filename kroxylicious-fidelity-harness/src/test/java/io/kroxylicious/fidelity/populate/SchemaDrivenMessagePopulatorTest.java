/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.populate;

import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
