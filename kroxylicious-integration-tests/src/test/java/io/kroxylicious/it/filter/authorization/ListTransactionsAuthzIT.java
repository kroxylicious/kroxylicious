/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListTransactionsRequestData;
import org.apache.kafka.common.message.ListTransactionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
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

import static org.assertj.core.api.Assertions.assertThat;

class ListTransactionsAuthzIT extends AuthzIT {

    public static final List<Short> SUPPORTED_API_VERSIONS = IntStream
            .rangeClosed(ApiKeys.LIST_TRANSACTIONS.oldestVersion(), ApiKeys.LIST_TRANSACTIONS.latestVersion(true)).boxed()
            .map(Integer::shortValue).toList();
    private Path rulesFile;

    private static final String ALICE_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX = "alice-transaction";
    private static final String BOB_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX = "bob-transaction";
    private static final List<String> ALL_TRANSACTIONAL_ID_PREFIXES = List.of(ALICE_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX, BOB_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX);
    private static List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TransactionalIdResource as TxnlId;
                allow User with name = "alice" to * TxnlId with name like "%s*";
                allow User with name = "bob" to DESCRIBE TxnlId with name like "%s*";
                allow User with name = "super" to * TxnlId with name like "*";
                otherwise deny;
                """.formatted(ALICE_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX, BOB_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(), List.of());
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(), List.of());
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        return SUPPORTED_API_VERSIONS.stream().<Arguments> map(
                apiVersion -> {
                    List<String> transactionIds = ALL_TRANSACTIONAL_ID_PREFIXES.stream().map(s -> s + "-" + UUID.randomUUID()).toList();
                    return Arguments.argumentSet("list offsets version " + apiVersion, new ListTransactionsEquivalence(apiVersion, transactionIds));
                }).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<ListOffsetsRequestData, ListOffsetsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class ListTransactionsEquivalence extends Equivalence<ListTransactionsRequestData, ListTransactionsResponseData> {

        private final List<String> transactionIds;

        ListTransactionsEquivalence(short apiVersion, List<String> transactionIds) {
            super(apiVersion);
            this.transactionIds = transactionIds;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.LIST_TRANSACTIONS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public ListTransactionsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return new ListTransactionsRequestData();
        }

        @Override
        public void assertUnproxiedResponses(Map<String, ListTransactionsResponseData> unproxiedResponsesByUser) {
            assertThat(unproxiedResponsesByUser.get(ALICE).transactionStates())
                    .isNotEmpty()
                    .allSatisfy(transactionState -> assertThat(transactionState.transactionalId()).startsWith(ALICE_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX));
            assertThat(unproxiedResponsesByUser.get(BOB).transactionStates())
                    .isNotEmpty()
                    .allSatisfy(transactionState -> assertThat(transactionState.transactionalId()).startsWith(BOB_TO_DESCRIBE_TRANSACTIONAL_ID_PREFIX));
            assertThat(unproxiedResponsesByUser.get(EVE).transactionStates()).isEmpty();
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            for (String id : transactionIds) {
                driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, id);
                driver.initProducerId(id);
            }
        }
    }

}
