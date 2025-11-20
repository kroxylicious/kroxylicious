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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
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

import io.kroxylicious.testing.kafka.junit5ext.Name;

public class ListOffsetsAuthzIT extends AuthzIT {

    public static final String EXISTING_TOPIC_NAME = "other-topic";
    public static final IntStream SUPPORTED_API_VERSIONS = IntStream.rangeClosed(ApiKeys.LIST_OFFSETS.oldestVersion(), ApiKeys.LIST_OFFSETS.latestVersion(true));
    public static final int NON_EXISTANT_PARTITION = 20;
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
        return SUPPORTED_API_VERSIONS.<Arguments> mapToObj(
                apiVersion -> Arguments.argumentSet("list offsets version " + apiVersion, new ListOffsetsEquivalence((short) apiVersion))).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<ListOffsetsRequestData, ListOffsetsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class ListOffsetsEquivalence extends Equivalence<ListOffsetsRequestData, ListOffsetsResponseData> {

        ListOffsetsEquivalence(short apiVersion) {
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
        public void assertUnproxiedResponses(Map<String, ListOffsetsResponseData> unproxiedResponsesByUser) {

        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.LIST_OFFSETS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public ListOffsetsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            ListOffsetsRequestData listOffsetsRequestData = new ListOffsetsRequestData();
            ListOffsetsRequestData.ListOffsetsTopic topicA = createListOffsetsTopic(ALICE_TO_DESCRIBE_TOPIC_NAME, 0, NON_EXISTANT_PARTITION);
            ListOffsetsRequestData.ListOffsetsTopic topicB = createListOffsetsTopic(BOB_TO_DESCRIBE_TOPIC_NAME, 0, NON_EXISTANT_PARTITION);
            ListOffsetsRequestData.ListOffsetsTopic topicC = createListOffsetsTopic(EXISTING_TOPIC_NAME, 0, NON_EXISTANT_PARTITION);
            listOffsetsRequestData.setTopics(List.of(topicA, topicB, topicC));
            return listOffsetsRequestData;
        }
    }

    private static ListOffsetsRequestData.ListOffsetsTopic createListOffsetsTopic(String topicName, int... partitions) {
        ListOffsetsRequestData.ListOffsetsTopic listOffsetsTopic = new ListOffsetsRequestData.ListOffsetsTopic();
        listOffsetsTopic.setName(topicName);
        List<ListOffsetsRequestData.ListOffsetsPartition> listOffsetsPartitions = Arrays.stream(partitions).mapToObj(partition -> {
            ListOffsetsRequestData.ListOffsetsPartition listOffsetsPartition = new ListOffsetsRequestData.ListOffsetsPartition();
            listOffsetsPartition.setPartitionIndex(partition);
            return listOffsetsPartition;
        }).toList();
        listOffsetsTopic.setPartitions(listOffsetsPartitions);
        return listOffsetsTopic;
    }
}
