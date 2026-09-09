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
        // "listeners" is an array-of-struct field with a custom ImplicitLinkedHashMultiCollection
        // setter rather than a plain List setter; that's a separate, out-of-scope case, so it's
        // stubbed out here to isolate the plain-struct field this test targets.
        FieldPopulationStrategy strategy = field -> field.def.name.equals("listeners")
                ? new FieldDecision.Value(new UpdateRaftVoterRequestData.ListenerCollection(0))
                : validScalarStrategy.resolve(field);
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(strategy);

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
    void unsupportedStructFieldInsideTaggedFieldsSectionExceptionNamesTheField() {
        // Given
        // node_endpoints is a struct-typed tag inside v1's non-empty tagged-fields section, a different,
        // map-keyed wire mechanism that struct recursion does not walk into.
        org.apache.kafka.common.message.BeginQuorumEpochResponseData instance = new org.apache.kafka.common.message.BeginQuorumEpochResponseData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        // Then
        assertThatThrownBy(() -> populator.populate(instance, (short) 1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("_tagged_fields");
    }

    @Test
    void wrapsSetterArgumentTypeMismatchWithFieldContext() {
        // Given
        // "listeners" is an array-of-struct field whose generated setter takes a custom
        // ImplicitLinkedHashMultiCollection type (ListenerCollection) rather than a plain List, so the
        // composed List<Object> value mismatches the setter's parameter type.
        UpdateRaftVoterRequestData instance = new UpdateRaftVoterRequestData();
        SchemaDrivenMessagePopulator populator = new SchemaDrivenMessagePopulator(validScalarStrategy);

        // When
        // Then
        assertThatThrownBy(() -> populator.populate(instance, (short) 0))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("listeners")
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
