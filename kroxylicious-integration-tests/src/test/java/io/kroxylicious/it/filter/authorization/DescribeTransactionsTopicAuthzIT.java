/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static io.kroxylicious.it.filter.authorization.ClusterPrepUtils.deleteTopicsAndAcls;
import static java.util.stream.Stream.concat;

class DescribeTransactionsTopicAuthzIT extends AuthzIT {
    private Path rulesFile;
    private List<AclBinding> aclBindings;

    private static final String TRANSACTION_PREFIX = "my-transaction";
    private static final String ALICE_TOPIC = "alice-topic";
    private static final String BOB_TOPIC = "bob-topic";
    private static final List<String> ALL_TOPICS = List.of(ALICE_TOPIC, BOB_TOPIC);

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;

    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic,
                                                                 TransactionalIdResource as TxnlId;
                allow User with name = "alice" to * TxnlId with name like "%s*";
                allow User with name = "bob" to * TxnlId with name like "%s*";
                allow User with name = "eve" to * TxnlId with name like "%s*";
                allow User with name = "super" to * TxnlId with name like "*";
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DESCRIBE Topic with name = "%s";
                allow User with name = "super" to * Topic with name like "*";
                otherwise deny;
                """.formatted(TRANSACTION_PREFIX, TRANSACTION_PREFIX, TRANSACTION_PREFIX, ALICE_TOPIC, BOB_TOPIC));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTION_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTION_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTION_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, ALL_TOPICS, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, ALL_TOPICS, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPICS, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPICS, List.of());
    }

    class DescribeTransactionsEquivalence extends Equivalence<DescribeTransactionsRequestData, DescribeTransactionsResponseData> {

        private final String transactionalId;

        DescribeTransactionsEquivalence(short apiVersion, String transactionalId) {
            super(apiVersion);
            this.transactionalId = transactionalId;
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
            requestData.transactionalIds().add(transactionalId);
            return requestData;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
            ProducerIdAndEpoch producerIdAndEpoch = driver.initProducerId(transactionalId);
            Map<String, Collection<Integer>> topicPartitionMap = ALL_TOPICS.stream().collect(Collectors.toMap(it -> it, it -> List.of(0)));
            driver.addPartitionsToTransaction(transactionalId, producerIdAndEpoch, topicPartitionMap);
            super.prepareCluster(cluster);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            clobber(jsonNodes);
            return prettyJsonString(jsonNodes);
        }

        private static void clobber(final JsonNode node) {
            if (node.isObject()) {
                clobberInt((ObjectNode) node, "transactionStartTimeMs", 0);
                sortArray((ObjectNode) node, "transactionStates", "transactionalId");
                node.values().forEachRemaining(DescribeTransactionsEquivalence::clobber);
            }
            if (node.isArray()) {
                node.forEach(DescribeTransactionsEquivalence::clobber);
            }
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.DESCRIBE_TRANSACTIONS),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.DESCRIBE_TRANSACTIONS))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " specific topics request",
                                new DescribeTransactionsEquivalence((short) (int) apiVersion, TRANSACTION_PREFIX + "-" + UUID.randomUUID()))));
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
