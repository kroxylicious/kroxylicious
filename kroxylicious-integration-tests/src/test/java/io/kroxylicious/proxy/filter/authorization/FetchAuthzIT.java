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
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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

import io.kroxylicious.testing.kafka.junit5ext.Name;

public class FetchAuthzIT extends AuthzIT {

    public static final String EXISTING_TOPIC_NAME = "other-topic";
    public static final IntStream API_VERSIONS_WITHOUT_TOPIC_IDS = IntStream.rangeClosed(4, 12);
    public static final IntStream API_VERSIONS_WITH_TOPIC_IDS = IntStream.rangeClosed(13, ApiKeys.FETCH.latestVersion(true));
    private Path rulesFile;

    private static final String ALICE_TO_READ_TOPIC_NAME = "alice-new-topic";
    private static final String BOB_TO_READ_TOPIC_NAME = "bob-new-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(EXISTING_TOPIC_NAME, ALICE_TO_READ_TOPIC_NAME, BOB_TO_READ_TOPIC_NAME);
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
                allow User with name = "bob" to READ Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TO_READ_TOPIC_NAME, BOB_TO_READ_TOPIC_NAME));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TO_READ_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TO_READ_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.READ, AclPermissionType.ALLOW)));
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
        Stream<Arguments> supportedVersions = API_VERSIONS_WITHOUT_TOPIC_IDS.mapToObj(
                apiVersion -> Arguments.argumentSet("fetch version " + apiVersion, new FetchEquivalence((short) apiVersion)));
        Stream<Arguments> unsupportedVersions = API_VERSIONS_WITH_TOPIC_IDS
                .mapToObj(apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion, new UnsupportedApiVersion<>(ApiKeys.FETCH, (short) apiVersion)));
        return Stream.concat(supportedVersions, unsupportedVersions)
                .toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<FetchRequestData, FetchResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class FetchEquivalence extends Equivalence<FetchRequestData, FetchResponseData> {

        FetchEquivalence(short apiVersion) {
            super(apiVersion);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {

        }

        @Override
        public void assertUnproxiedResponses(Map<String, FetchResponseData> unproxiedResponsesByUser) {

        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.FETCH;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public FetchRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            FetchRequestData fetchRequestData = new FetchRequestData();
            FetchRequestData.FetchTopic topicA = createFetchTopic(ALICE_TO_READ_TOPIC_NAME, 0);
            FetchRequestData.FetchTopic topicB = createFetchTopic(BOB_TO_READ_TOPIC_NAME, 0);
            FetchRequestData.FetchTopic topicC = createFetchTopic(EXISTING_TOPIC_NAME, 0);
            fetchRequestData.setTopics(List.of(topicA, topicB, topicC));
            return fetchRequestData;
        }

        @Override
        public boolean needsRetry(FetchResponseData r) {
            Errors errors = Errors.forCode(r.errorCode());
            boolean anyPartitionError = r.responses().stream()
                    .flatMap(topicData -> topicData.partitions().stream())
                    .anyMatch(partitionData -> Errors.forCode(partitionData.errorCode()) == Errors.NOT_LEADER_OR_FOLLOWER);
            return errors == Errors.NOT_LEADER_OR_FOLLOWER || anyPartitionError;
        }
    }

    private static FetchRequestData.FetchTopic createFetchTopic(String topicName, int... partitions) {
        FetchRequestData.FetchTopic fetchTopic = new FetchRequestData.FetchTopic();
        fetchTopic.setTopic(topicName);
        List<FetchRequestData.FetchPartition> fetchPartitions = Arrays.stream(partitions).mapToObj(partition -> {
            FetchRequestData.FetchPartition fetchPartition = new FetchRequestData.FetchPartition();
            fetchPartition.setPartition(partition);
            return fetchPartition;
        }).toList();
        fetchTopic.setPartitions(fetchPartitions);
        return fetchTopic;
    }
}
