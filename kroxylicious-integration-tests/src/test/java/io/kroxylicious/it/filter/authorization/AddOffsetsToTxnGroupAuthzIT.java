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
import java.util.UUID;
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

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.deleteTopicsAndAcls;
import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

class AddOffsetsToTxnGroupAuthzIT extends AuthzIT {

    public static final String TRANSACTIONAL_ID_PREFIX = "transaction-";
    public static final String GROUP_ID = "group";
    public static final String ALICE_GROUP_PREFIX = "alice";
    public static final String BOB_GROUP_PREFIX = "bob";
    public static final List<String> ALL_GROUP_PREFIXES = List.of(ALICE_GROUP_PREFIX, BOB_GROUP_PREFIX);
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
                from io.kroxylicious.filter.authorization import GroupResource as Group;

                allow User with name = "alice" to * Group with name like "%s-*";
                allow User with name = "bob" to READ Group with name like "%s-*";
                allow User with name = "super" to * Group with name like "*";
                otherwise deny;
                """.formatted(ALICE_GROUP_PREFIX, BOB_GROUP_PREFIX));

        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.READ, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TRANSACTIONAL_ID_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + EVE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)));
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
                .flatMap(apiVersion -> ALL_GROUP_PREFIXES.stream().map(
                        groupPrefix -> Arguments.argumentSet("api version " + apiVersion + " groupPrefix " + groupPrefix,
                                new AddOffsetsToTxnEquivalence(apiVersion.shortValue(), groupPrefix))));
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

        private final String group;
        private final String transactionalId = TRANSACTIONAL_ID_PREFIX + "-" + UUID.randomUUID();
        private final String groupPrefix;
        private ProducerIdAndEpoch producerIdAndEpoch;

        AddOffsetsToTxnEquivalence(short apiVersion, String groupPrefix) {
            super(apiVersion);
            this.groupPrefix = groupPrefix;
            this.group = this.groupPrefix + "-" + UUID.randomUUID();
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
            request.setProducerId(Objects.requireNonNull(producerIdAndEpoch).producerId);
            request.setGroupId(group);
            request.setProducerEpoch(producerIdAndEpoch.epoch);
            return request;
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, group);
            producerIdAndEpoch = driver.initProducerId(transactionalId);
            super.prepareCluster(cluster);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, AddOffsetsToTxnResponseData> unproxiedResponsesByUser) {
            Errors eveError = Errors.forCode(unproxiedResponsesByUser.get(EVE).errorCode());
            Errors aliceError = Errors.forCode(unproxiedResponsesByUser.get(ALICE).errorCode());
            Errors bobError = Errors.forCode(unproxiedResponsesByUser.get(BOB).errorCode());
            assertThat(eveError).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
            if (groupPrefix.equals(ALICE_GROUP_PREFIX)) {
                assertThat(aliceError).isEqualTo(Errors.NONE);
                assertThat(bobError).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
            }
            else if (groupPrefix.equals(BOB_GROUP_PREFIX)) {
                assertThat(aliceError).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
                assertThat(bobError).isEqualTo(Errors.NONE);
            }
        }
    }

}
