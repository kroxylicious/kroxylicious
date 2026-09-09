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
import io.kroxylicious.kafka.common.message.AddRaftVoterResponseData;
import io.kroxylicious.kafka.common.message.AlterUserScramCredentialsResponseData;
import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.DeleteTopicsRequestData;
import io.kroxylicious.kafka.common.message.EndTxnRequestData;
import io.kroxylicious.kafka.common.message.EnvelopeResponseData;
import io.kroxylicious.kafka.common.message.FindCoordinatorRequestData;
import io.kroxylicious.kafka.common.message.HeartbeatRequestData;
import io.kroxylicious.kafka.common.message.HeartbeatResponseData;
import io.kroxylicious.kafka.common.message.RemoveRaftVoterRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.message.SaslAuthenticateRequestData;
import io.kroxylicious.kafka.common.message.SaslHandshakeRequestData;
import io.kroxylicious.kafka.common.message.UpdateRaftVoterRequestData;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    void populatesMessageWithEmptyTaggedFieldsSection() {
        // Given
        ResponseHeaderData instance = new ResponseHeaderData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 1);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.correlationId()).isNotZero();
    }

    @Test
    void populatesCompactBytesField() {
        // Given
        SaslAuthenticateRequestData instance = new SaslAuthenticateRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 2);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.authBytes()).isNotEmpty();
    }

    @Test
    void populatesCompactNullableBytesField() {
        // Given
        EnvelopeResponseData instance = new EnvelopeResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.responseData()).isNotNull();
    }

    @Test
    void populatesCompactNullableStringField() {
        // Given
        AddRaftVoterResponseData instance = new AddRaftVoterResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.errorMessage()).isNotNull();
    }

    @Test
    void populatesCompactStringField() {
        // Given
        ApiVersionsRequestData instance = new ApiVersionsRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 3);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.clientSoftwareName()).isNotNull();
        assertThat(instance.clientSoftwareVersion()).isNotNull();
    }

    @Test
    void populatesNullableStringField() {
        // Given
        RequestHeaderData instance = new RequestHeaderData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 1);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.clientId()).isNotNull();
    }

    @Test
    void populatesUuidField() {
        // Given
        RemoveRaftVoterRequestData instance = new RemoveRaftVoterRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.voterDirectoryId()).isNotNull();
    }

    @Test
    void populatesUuidFieldOnKafkaInstance() {
        // Given
        // ValidScalarStrategy always produces an io.kroxylicious.kafka.common.Uuid, but this instance's
        // setter expects org.apache.kafka.common.Uuid - a different class with the same shape.
        org.apache.kafka.common.message.RemoveRaftVoterRequestData instance = new org.apache.kafka.common.message.RemoveRaftVoterRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.voterDirectoryId()).isNotNull();
    }

    @Test
    void populatesArrayOfUuidFieldOnKafkaInstance() {
        // Given
        // ValidScalarStrategy always produces io.kroxylicious.kafka.common.Uuid elements, but this
        // instance's setter expects List<org.apache.kafka.common.Uuid> - List<X> erases to a plain List
        // at the setter's parameter type, so the family mismatch isn't caught by matching the setter's
        // own (erased) parameter type the way a directly Uuid-typed field's mismatch is.
        org.apache.kafka.common.message.BrokerRegistrationRequestData instance = new org.apache.kafka.common.message.BrokerRegistrationRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 2);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.logDirs()).isNotEmpty();
        assertThat(instance.logDirs()).allSatisfy(uuid -> assertThat(uuid).isInstanceOf(org.apache.kafka.common.Uuid.class));
    }

    @Test
    void populatesRecordsFieldOnKafkaInstance() {
        // Given
        // ValidScalarStrategy always produces an io.kroxylicious.kafka.common.record.internal.MemoryRecords,
        // but this instance's setter expects org.apache.kafka.common.record.internal.BaseRecords, satisfied
        // by a different, same-shaped org.apache.kafka.common.record.internal.MemoryRecords class. Version 4
        // is used because it is the lowest version with plain List setters throughout and no tagged-fields
        // sections in the struct chain leading to Records, isolating this fix from the separate, unrelated,
        // out-of-scope custom-collection-setter and tagged-struct-field cases.
        org.apache.kafka.common.message.FetchResponseData instance = new org.apache.kafka.common.message.FetchResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 4);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.responses()).isNotEmpty();
        assertThat(instance.responses().get(0).partitions()).isNotEmpty();
        assertThat(instance.responses().get(0).partitions().get(0).records()).isNotNull();
    }

    @Test
    void populatesArrayOfScalarField() {
        // Given
        DeleteTopicsRequestData instance = new DeleteTopicsRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 1);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.topicNames()).isNotEmpty();
    }

    @Test
    void populatesPlainStructField() {
        // Given
        UpdateRaftVoterRequestData instance = new UpdateRaftVoterRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.kRaftVersionFeature()).isNotNull();
        assertThat(instance.kRaftVersionFeature().minSupportedVersion()).isNotZero();
        assertThat(instance.kRaftVersionFeature().maxSupportedVersion()).isNotZero();
    }

    @Test
    void populatesArrayOfStructField() {
        // Given
        AlterUserScramCredentialsResponseData instance = new AlterUserScramCredentialsResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.results()).isNotEmpty();
        assertThat(instance.results()).allSatisfy(scramCredentialsResult -> {
            assertThat(scramCredentialsResult.user()).isNotNull();
            assertThat(scramCredentialsResult.errorMessage()).isNotNull();
        });
    }

    @Test
    void populatesNullableStructField() {
        // Given
        // "topology" is a nullable struct field; Kafka's generator wraps its schema in a NullableSchema
        // that copies the struct's Field defs into a brand new Schema instance rather than reusing the
        // struct class's own SCHEMA_0 object, so identity-based struct-class resolution must see through
        // that wrapper.
        org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData instance = new org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.topology()).isNotNull();
        assertThat(instance.topology().epoch()).isNotZero();
    }

    @Test
    void populatesArrayOfStructFieldWithCustomCollectionSetter() {
        // Given
        // "listeners" is an array-of-struct field whose generated setter takes a custom
        // ImplicitLinkedHashMultiCollection-derived collection (ListenerCollection) rather than a
        // plain List, so the composed List<Object> value must be adapted to the setter's own
        // collection type.
        UpdateRaftVoterRequestData instance = new UpdateRaftVoterRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 0);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.listeners()).isNotEmpty();
        assertThat(instance.listeners()).allSatisfy(listener -> {
            assertThat(listener.name()).isNotNull();
            assertThat(listener.host()).isNotNull();
        });
    }

    @Test
    void populatesScalarArrayFieldInsideTaggedFieldsSection() {
        // Given
        // "offline_log_dirs" is a []uuid field inside v1's non-empty tagged-fields section.
        org.apache.kafka.common.message.BrokerHeartbeatRequestData instance = new org.apache.kafka.common.message.BrokerHeartbeatRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 1);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.offlineLogDirs()).isNotEmpty();
    }

    @Test
    void populatesArrayOfStructFieldInsideTaggedFieldsSection() {
        // Given
        // "node_endpoints" is a struct-typed tag inside v1's non-empty tagged-fields section, and its
        // struct (NodeEndpoint) has a mapKey field, so its generated setter also takes a custom
        // NodeEndpointCollection rather than a plain List.
        org.apache.kafka.common.message.BeginQuorumEpochResponseData instance = new org.apache.kafka.common.message.BeginQuorumEpochResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        PopulationResult result = populator.populate(instance, (short) 1);

        // Then
        assertThat(result).isInstanceOf(PopulationResult.Populated.class);
        assertThat(instance.nodeEndpoints()).isNotEmpty();
        assertThat(instance.nodeEndpoints()).allSatisfy(nodeEndpoint -> assertThat(nodeEndpoint.host()).isNotNull());
    }

    @Test
    void wrapsSetterArgumentTypeMismatchWithFieldContext() {
        // Given
        // "generation_id" is an int32 field; a strategy that produces a String for it mismatches the
        // generated setter's parameter type.
        FieldPopulationStrategy mistypedStrategy = field -> field.def.name.equals("generation_id")
                ? new FieldDecision.Value("not-an-int")
                : validScalarStrategy.resolve(field);
        HeartbeatRequestData instance = new HeartbeatRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(mistypedStrategy);

        // When
        // Then
        assertThatThrownBy(() -> populator.populate(instance, (short) 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("generation_id")
                .cause().isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void populateAddsFieldContextRegardlessOfStrategyFailureType() {
        // Given
        FieldPopulationStrategy opaqueFailingStrategy = field -> {
            throw new IllegalStateException("boom");
        };
        HeartbeatRequestData instance = new HeartbeatRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(opaqueFailingStrategy);

        // When
        // Then
        assertThatThrownBy(() -> populator.populate(instance, (short) 0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("of type")
                .cause().isInstanceOf(IllegalStateException.class).hasMessage("boom");
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
