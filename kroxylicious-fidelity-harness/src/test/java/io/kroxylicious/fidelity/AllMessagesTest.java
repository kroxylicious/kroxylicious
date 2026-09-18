/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.VoteRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.kroxylicious.fidelity.AllMessages.VersionedMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class AllMessagesTest {

    @Test
    void includesRequestAndResponseAcrossFullVersionRangeForAClientApi() {
        // Given
        HeartbeatRequestData template = new HeartbeatRequestData();
        List<Short> supportedVersions = IntStream.rangeClosed(template.lowestSupportedVersion(), template.highestSupportedVersion())
                .mapToObj(version -> (short) version)
                .toList();

        // When
        List<VersionedMessage> heartbeat = AllMessages.stream()
                .filter(versionedMessage -> versionedMessage.messageName().equals("Heartbeat"))
                .toList();

        // Then
        assertThat(heartbeat)
                .extracting(VersionedMessage::direction, VersionedMessage::version)
                .containsExactlyInAnyOrderElementsOf(
                        supportedVersions.stream()
                                .flatMap(version -> Stream.of(tuple("Request", version), tuple("Response", version)))
                                .toList());
    }

    @Test
    void includesControllerOnlyApisNotPresentInClientApis() {
        // Given a controller-scoped API with no client-facing counterpart
        VoteRequestData template = new VoteRequestData();
        assertThat(ApiKeys.clientApis()).doesNotContain(ApiKeys.VOTE);
        assertThat(ApiKeys.controllerApis()).contains(ApiKeys.VOTE);

        // When
        List<VersionedMessage> vote = AllMessages.stream()
                .filter(versionedMessage -> versionedMessage.messageName().equals("Vote"))
                .toList();

        // Then
        assertThat(vote)
                .isNotEmpty()
                .allSatisfy(versionedMessage -> assertThat(versionedMessage.version())
                        .isBetween(template.lowestSupportedVersion(), template.highestSupportedVersion()));
    }

    @Test
    void doesNotDuplicateApisPresentInBothClientAndControllerScopes() {
        // Given a controller-scoped API that is also client-facing
        assertThat(ApiKeys.controllerApis()).contains(ApiKeys.FETCH);
        assertThat(ApiKeys.clientApis()).contains(ApiKeys.FETCH);

        // When
        List<VersionedMessage> fetch = AllMessages.stream()
                .filter(versionedMessage -> versionedMessage.messageName().equals("Fetch"))
                .toList();

        // Then
        assertThat(fetch)
                .extracting(VersionedMessage::direction, VersionedMessage::version)
                .doesNotHaveDuplicates();
    }

    @Test
    void everyVersionedMessageHasItsOwnFreshInstances() {
        // When
        List<VersionedMessage> heartbeat = AllMessages.stream()
                .filter(versionedMessage -> versionedMessage.messageName().equals("Heartbeat"))
                .toList();

        // Then
        assertThat(distinctByIdentity(heartbeat.stream().map(VersionedMessage::kroxyliciousMessage))).isEqualTo(heartbeat.size());
        assertThat(distinctByIdentity(heartbeat.stream().map(VersionedMessage::kafkaMessage))).isEqualTo(heartbeat.size());
    }

    private static int distinctByIdentity(Stream<?> instances) {
        Set<Object> identitySet = Collections.newSetFromMap(new IdentityHashMap<>());
        instances.forEach(identitySet::add);
        return identitySet.size();
    }

    @Test
    void labelCombinesMessageNameDirectionAndVersion() {
        // Given
        VersionedMessage versionedMessage = new VersionedMessage("Heartbeat", "Request", (short) 2,
                new io.kroxylicious.kafka.common.message.HeartbeatRequestData(), new HeartbeatRequestData());

        // When
        String label = versionedMessage.label();

        // Then
        assertThat(label).isEqualTo("HeartbeatRequest - v2");
    }
}
