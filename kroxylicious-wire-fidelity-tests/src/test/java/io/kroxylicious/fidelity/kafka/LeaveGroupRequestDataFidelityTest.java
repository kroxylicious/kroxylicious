/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity.kafka;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.fidelity.FidelityCheck;
import io.kroxylicious.fidelity.ReadResult;
import io.kroxylicious.kafka.common.message.LeaveGroupRequestData;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves {@link LeaveGroupRequestData} can be read correctly by, and can correctly read messages from,
 * {@code org.apache.kafka.common.message.LeaveGroupRequestData}, at every version its {@code members}
 * field (a nested-struct-in-list field, as opposed to {@code RequestHeaderData}'s flat primitive/string
 * fields) supports: version 3 (plain array, non-compact strings), version 4 (compact array, compact
 * strings, tagged fields), and version 5 (adds the nested {@code reason} field).
 */
class LeaveGroupRequestDataFidelityTest {

    static Stream<Short> supportedVersions() {
        return Stream.of((short) 3, (short) 4, (short) 5);
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    void kroxyliciousShouldReadKafkaSerialisedMessage(short version) {
        // Given
        String reason = version >= 5 ? "leaving" : null;
        org.apache.kafka.common.message.LeaveGroupRequestData kafkaSource = new org.apache.kafka.common.message.LeaveGroupRequestData()
                .setGroupId("grp")
                .setMembers(List.of(
                        new org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity().setMemberId("m1").setReason(reason),
                        new org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity().setMemberId("m2").setGroupInstanceId("gi").setReason(reason)));

        // When
        ReadResult<LeaveGroupRequestData> result = FidelityCheck.kroxyliciousReads(kafkaSource, new LeaveGroupRequestData(), version);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(kafkaSource);
    }

    @ParameterizedTest
    @MethodSource("supportedVersions")
    void kafkaShouldReadKroxyliciousSerialisedMessage(short version) {
        // Given
        String reason = version >= 5 ? "leaving" : null;
        LeaveGroupRequestData oursSource = new LeaveGroupRequestData()
                .setGroupId("grp")
                .setMembers(List.of(
                        new LeaveGroupRequestData.MemberIdentity().setMemberId("m1").setReason(reason),
                        new LeaveGroupRequestData.MemberIdentity().setMemberId("m2").setGroupInstanceId("gi").setReason(reason)));

        // When
        ReadResult<org.apache.kafka.common.message.LeaveGroupRequestData> result = FidelityCheck.kafkaReads(
                oursSource, new org.apache.kafka.common.message.LeaveGroupRequestData(), version);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(oursSource);
    }
}
