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
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTransactionsRequestData;
import org.apache.kafka.common.message.DescribeTransactionsResponseData;
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

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.deleteTopicsAndAcls;
import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static java.util.stream.Stream.concat;

class DescribeTransactionsTxnlIdAuthzIT extends AuthzIT {
    private Path rulesFile;
    private List<AclBinding> aclBindings;

    private static final String ALICE_TRANSACTION_PREFIX = "alice-transaction";
    private static final String BOB_TRANSACTION_PREFIX = "bob-transaction";

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
                """.formatted(ALICE_TRANSACTION_PREFIX, BOB_TRANSACTION_PREFIX));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TRANSACTION_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TRANSACTION_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, List.of(), aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, List.of(), List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, List.of(), aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, List.of(), List.of());
    }

    class DescribeTransactionsEquivalence extends Equivalence<DescribeTransactionsRequestData, DescribeTransactionsResponseData> {

        private final List<String> transactionalIds;

        DescribeTransactionsEquivalence(short apiVersion, List<String> transactionalIds) {
            super(apiVersion);
            this.transactionalIds = transactionalIds;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.DESCRIBE_TRANSACTIONS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public DescribeTransactionsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            DescribeTransactionsRequestData requestData = new DescribeTransactionsRequestData();
            requestData.transactionalIds().addAll(transactionalIds);
            return requestData;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            for (String id : transactionalIds) {
                driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, id);
                driver.initProducerId(id);
            }

            super.prepareCluster(cluster);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(sortArray(jsonNodes, "transactionStates", "transactionalId"));
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        List<String> transactionalIds = List.of(ALICE_TRANSACTION_PREFIX + "-" + UUID.randomUUID(), BOB_TRANSACTION_PREFIX + "-" + UUID.randomUUID());
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.DESCRIBE_TRANSACTIONS),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.DESCRIBE_TRANSACTIONS))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " specific topics request",
                                new DescribeTransactionsEquivalence((short) (int) apiVersion, transactionalIds))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.DESCRIBE_TRANSACTIONS.oldestVersion(), ApiKeys.DESCRIBE_TRANSACTIONS.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.DESCRIBE_TRANSACTIONS, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.DESCRIBE_TRANSACTIONS, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<DescribeTopicPartitionsRequestData, DescribeTopicPartitionsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
