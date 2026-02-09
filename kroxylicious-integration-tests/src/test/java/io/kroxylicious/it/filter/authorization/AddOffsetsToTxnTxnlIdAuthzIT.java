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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.AddOffsetsToTxnResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
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

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.deleteTopicsAndAcls;
import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static java.util.stream.Stream.concat;

class AddOffsetsToTxnTxnlIdAuthzIT extends AuthzIT {

    public static final String TRANSACTIONAL_ID_SUFFIX = "-transactionalId";
    public static final String BOB_TXNL_ID = BOB + TRANSACTIONAL_ID_SUFFIX;
    public static final String ALICE_TXNL_ID = ALICE + TRANSACTIONAL_ID_SUFFIX;
    public static final String GROUP_ID = "group";
    private Path rulesFile;

    public static final List<String> ALL_TRANSACTIONAL_IDS_IN_TEST = List.of(ALICE_TXNL_ID, BOB_TXNL_ID);
    private static List<AclBinding> aclBindings;

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

                allow User with name = "alice" to * TxnlId with name = "%s";
                allow User with name = "bob" to {DESCRIBE, WRITE} TxnlId with name = "%s";
                allow User with name = "super" to * TxnlId with name like "*";
                otherwise deny;
                """.formatted(ALICE_TXNL_ID, BOB_TXNL_ID));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TXNL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TXNL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TXNL_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.WRITE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        try {
            prepCluster(kafkaClusterWithAuthz, List.of(), aclBindings);
            prepCluster(kafkaClusterNoAuthz, List.of(), List.of());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, List.of(), aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, List.of(), List.of());
    }

    List<Arguments> shouldEnforceAccessToTransactionalIds() {
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(
                AuthorizationFilter.minSupportedApiVersion(ApiKeys.ADD_OFFSETS_TO_TXN),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.ADD_OFFSETS_TO_TXN)).boxed()
                .flatMap(apiVersion -> ALL_TRANSACTIONAL_IDS_IN_TEST.stream().map(
                        s -> Arguments.argumentSet("api version " + apiVersion + " transactionalId " + s, new AddOffsetsToTxnEquivalence(apiVersion.shortValue(), s))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(
                        ApiKeys.ADD_OFFSETS_TO_TXN.oldestVersion(),
                        ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.ADD_OFFSETS_TO_TXN, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.ADD_OFFSETS_TO_TXN, (short) apiVersion)));
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

    class AddOffsetsToTxnEquivalence extends Equivalence<AddOffsetsToTxnRequestData, AddOffsetsToTxnResponseData> {

        private final String transactionalId;

        AddOffsetsToTxnEquivalence(short apiVersion, String transactionalId) {
            super(apiVersion);
            this.transactionalId = transactionalId;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            return prettyJsonString(jsonResponse);
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.ADD_OFFSETS_TO_TXN;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public AddOffsetsToTxnRequestData requestData(String user, BaseClusterFixture clusterFixture) {

            AddOffsetsToTxnRequestData request = new AddOffsetsToTxnRequestData();
            request.setTransactionalId(transactionalId);
            request.setProducerId(1L);
            request.setGroupId(GROUP_ID);
            request.setProducerEpoch((short) 1);
            return request;
        }

    }

}
