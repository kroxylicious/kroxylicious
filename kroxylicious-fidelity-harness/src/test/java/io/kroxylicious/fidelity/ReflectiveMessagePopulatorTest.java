/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.message.LeaveGroupRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;

import static org.assertj.core.api.Assertions.assertThat;

class ReflectiveMessagePopulatorTest {

    @Test
    void shouldPopulateFlatPrimitiveAndStringFields() {
        // Given
        RequestHeaderData message = new RequestHeaderData();

        // When
        ReflectiveMessagePopulator.populate(message, 42L);

        // Then
        assertThat(message.requestApiKey()).isNotZero();
        assertThat(message.requestApiVersion()).isNotZero();
        assertThat(message.correlationId()).isNotZero();
        assertThat(message.clientId()).isNotNull().isNotEmpty();
    }

    @Test
    void shouldPopulateKafkaNamespaceFieldsTheSameWay() {
        // Given
        org.apache.kafka.common.message.RequestHeaderData message = new org.apache.kafka.common.message.RequestHeaderData();

        // When
        ReflectiveMessagePopulator.populate(message, 42L);

        // Then
        assertThat(message.requestApiKey()).isNotZero();
        assertThat(message.requestApiVersion()).isNotZero();
        assertThat(message.correlationId()).isNotZero();
        assertThat(message.clientId()).isNotNull().isNotEmpty();
    }

    @Test
    void shouldPopulateNestedStructListFields() {
        // Given
        LeaveGroupRequestData message = new LeaveGroupRequestData();

        // When
        ReflectiveMessagePopulator.populate(message, 42L);

        // Then
        assertThat(message.groupId()).isNotNull().isNotEmpty();
        List<LeaveGroupRequestData.MemberIdentity> members = message.members();
        assertThat(members).isNotEmpty();
        assertThat(members).allSatisfy(member -> assertThat(member.memberId()).isNotNull().isNotEmpty());
    }

    @Test
    void shouldNotFabricateUnknownTaggedFields() {
        // Given
        RequestHeaderData message = new RequestHeaderData();

        // When
        ReflectiveMessagePopulator.populate(message, 42L);

        // Then
        assertThat(message.unknownTaggedFields()).isEmpty();
    }

    @Test
    void shouldBeReproducibleForTheSameSeed() {
        // Given
        RequestHeaderData first = new RequestHeaderData();
        RequestHeaderData second = new RequestHeaderData();

        // When
        ReflectiveMessagePopulator.populate(first, 42L);
        ReflectiveMessagePopulator.populate(second, 42L);

        // Then
        assertThat(second).usingRecursiveComparison().isEqualTo(first);
    }
}
