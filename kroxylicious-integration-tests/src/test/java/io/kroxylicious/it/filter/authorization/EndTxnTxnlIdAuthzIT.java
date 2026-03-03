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
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static io.kroxylicious.it.filter.authorization.ClusterPrepUtils.deleteTopicsAndAcls;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

class EndTxnTxnlIdAuthzIT extends AuthzIT {

    private static final String ALICE_TXNL_ID_PREFIX = "alice-transactionalId";
    private static final String BOB_TXNL_ID_PREFIX = "bob-transactionalId";
    private static final List<String> ALL_TRANSACTIONAL_ID_PREFIXES = List.of(ALICE_TXNL_ID_PREFIX, BOB_TXNL_ID_PREFIX);
    public static final String TOPIC_NAME = "my-topic";
    private Path rulesFile;

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
                allow User with name = "bob" to WRITE TxnlId with name like "%s*";
                allow User with name = "super" to * TxnlId with name like "*";
                otherwise deny;
                """.formatted(ALICE_TXNL_ID_PREFIX, BOB_TXNL_ID_PREFIX));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TXNL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TXNL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.WRITE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        try {
            this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, List.of(TOPIC_NAME), aclBindings);
            this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, List.of(TOPIC_NAME), List.of());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(TOPIC_NAME), aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(TOPIC_NAME), List.of());
    }

    List<Arguments> shouldEnforceAccessToTransactionalIds() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(
                AuthorizationFilter.minSupportedApiVersion(ApiKeys.END_TXN),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.END_TXN))
                .boxed()
                .flatMap(apiVersion -> ALL_TRANSACTIONAL_ID_PREFIXES.stream()
                        .map(prefix -> Arguments.argumentSet("api version " + apiVersion + " transactional id prefix " + prefix,
                                new EndTransactionEquivalence(apiVersion.shortValue(), prefix + "-" + UUID.randomUUID()))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(
                        ApiKeys.END_TXN.oldestVersion(),
                        ApiKeys.END_TXN.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.END_TXN, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.END_TXN, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTransactionalIds(VersionSpecificVerification<TxnOffsetCommitRequestData, TxnOffsetCommitResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

    class EndTransactionEquivalence extends Equivalence<EndTxnRequestData, EndTxnResponseData> {

        private final String transactionalId;
        private ProducerIdAndEpoch producerIdAndEpoch;

        EndTransactionEquivalence(short apiVersion, String transactionalId) {
            super(apiVersion);
            this.transactionalId = transactionalId;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.END_TXN;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public EndTxnRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return new EndTxnRequestData().setTransactionalId(transactionalId)
                    .setCommitted(true)
                    .setProducerEpoch(Objects.requireNonNull(producerIdAndEpoch).epoch)
                    .setProducerId(producerIdAndEpoch.producerId);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, EndTxnResponseData> unproxiedResponsesByUser) {
            Map<String, Errors> errorsPerUser = unproxiedResponsesByUser.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> Errors.forCode(e.getValue().errorCode())));
            if (transactionalId.startsWith(ALICE_TXNL_ID_PREFIX)) {
                assertThat(errorsPerUser).containsEntry(ALICE, Errors.NONE)
                        .containsEntry(BOB, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
                        .containsEntry(EVE, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
            }
            else if (transactionalId.startsWith(BOB_TXNL_ID_PREFIX)) {
                assertThat(errorsPerUser).containsEntry(ALICE, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
                        .containsEntry(BOB, Errors.NONE)
                        .containsEntry(EVE, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
            }
            else {
                throw new IllegalStateException("unexpected transactionalId under test " + transactionalId);
            }
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
            producerIdAndEpoch = driver.initProducerId(transactionalId);
            driver.addPartitionsToTransaction(transactionalId, producerIdAndEpoch, Map.of(TOPIC_NAME, Set.of(0)));
        }
    }

}
