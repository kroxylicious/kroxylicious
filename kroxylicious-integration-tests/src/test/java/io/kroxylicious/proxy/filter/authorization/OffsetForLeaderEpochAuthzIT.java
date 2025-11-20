/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData;
import org.apache.kafka.common.message.OffsetForLeaderEpochRequestData.OffsetForLeaderTopic;
import org.apache.kafka.common.message.OffsetForLeaderEpochResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static java.util.stream.Stream.concat;

public class OffsetForLeaderEpochAuthzIT extends AuthzIT {

    public static final String EXISTING_TOPIC_NAME = "other-topic";
    private Path rulesFile;

    private static final String ALICE_TO_DESCRIBE_TOPIC_NAME = "alice-new-topic";
    private static final String BOB_TO_DESCRIBE_TOPIC_NAME = "bob-new-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(EXISTING_TOPIC_NAME, ALICE_TO_DESCRIBE_TOPIC_NAME, BOB_TO_DESCRIBE_TOPIC_NAME);
    private static List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DESCRIBE Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TO_DESCRIBE_TOPIC_NAME, BOB_TO_DESCRIBE_TOPIC_NAME));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TO_DESCRIBE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TO_DESCRIBE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.OFFSET_FOR_LEADER_EPOCH))
                .mapToObj(apiVersion -> Arguments.argumentSet("api version before batching version " + apiVersion, new OffsetFetchEquivalence((short) apiVersion)));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.OFFSET_FOR_LEADER_EPOCH.oldestVersion(), ApiKeys.OFFSET_FOR_LEADER_EPOCH.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.OFFSET_FOR_LEADER_EPOCH, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.OFFSET_FOR_LEADER_EPOCH, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class OffsetFetchEquivalence extends Equivalence<OffsetForLeaderEpochRequestData, OffsetForLeaderEpochResponseData> {

        OffsetFetchEquivalence(short apiVersion) {
            super(apiVersion);
        }

        @Override
        public String clobberResponse(BaseClusterFixture clusterFixture, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {

        }

        @Override
        public void assertUnproxiedResponses(Map<String, OffsetForLeaderEpochResponseData> unproxiedResponsesByUser) {

        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.OFFSET_FOR_LEADER_EPOCH;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public OffsetForLeaderEpochRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            OffsetForLeaderEpochRequestData offsetForLeaderEpochRequestData = new OffsetForLeaderEpochRequestData();
            OffsetForLeaderTopic topicA = createOffsetForLeaderTopic(ALICE_TO_DESCRIBE_TOPIC_NAME, 0, 20);
            OffsetForLeaderTopic topicB = createOffsetForLeaderTopic(BOB_TO_DESCRIBE_TOPIC_NAME, 0, 20);
            OffsetForLeaderTopic topicC = createOffsetForLeaderTopic(EXISTING_TOPIC_NAME, 0, 20);
            offsetForLeaderEpochRequestData.topics().addAll(List.of(topicA, topicB, topicC));
            return offsetForLeaderEpochRequestData;
        }
    }

    private static OffsetForLeaderTopic createOffsetForLeaderTopic(String topicName, int... partitions) {
        OffsetForLeaderTopic fetchTopic = new OffsetForLeaderTopic();
        fetchTopic.setTopic(topicName);
        fetchTopic.setPartitions(
                Arrays.stream(partitions).boxed().map(partition -> new OffsetForLeaderEpochRequestData.OffsetForLeaderPartition().setPartition(partition)).toList());
        return fetchTopic;
    }

}
