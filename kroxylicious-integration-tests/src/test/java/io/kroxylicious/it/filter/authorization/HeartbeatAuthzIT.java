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
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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

import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

public class HeartbeatAuthzIT extends AuthzIT {

    private final String ALICE_GROUP_PREFIX = "alice-group";
    private final String BOB_GROUP_PREFIX = "bob-group";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of();

    private Path rulesFile;

    private List<AclBinding> aclBindings;

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
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP_PREFIX, PatternType.PREFIXED),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.READ, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    class HeartbeatEquivalence extends Equivalence<HeartbeatRequestData, HeartbeatResponseData> {

        private final String group;
        private final String groupPrefix;
        private String groupInstanceId;
        private String memberId;
        private Integer generationId;

        HeartbeatEquivalence(short apiVersion, String groupPrefix) {
            super(apiVersion);
            this.group = groupPrefix + "-" + UUID.randomUUID();
            this.groupPrefix = groupPrefix;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.HEARTBEAT;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public HeartbeatRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            HeartbeatRequestData request = new HeartbeatRequestData();
            request.setGroupId(Objects.requireNonNull(group));
            request.setGenerationId(Objects.requireNonNull(generationId));
            request.setMemberId(Objects.requireNonNull(memberId));
            if (apiVersion() >= 3) {
                request.setGroupInstanceId(groupInstanceId);
            }
            return request;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, group);
            groupInstanceId = group + "-" + UUID.randomUUID();
            JoinGroupResponseData responseData = driver.joinGroup("consumer", group, groupInstanceId);
            memberId = responseData.memberId();
            generationId = responseData.generationId();
            driver.syncGroup(group, groupInstanceId, "consumer", generationId, memberId);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, HeartbeatResponseData> unproxiedResponsesByUser) {
            Errors aliceError = Errors.forCode(unproxiedResponsesByUser.get(ALICE).errorCode());
            Errors bobError = Errors.forCode(unproxiedResponsesByUser.get(BOB).errorCode());
            Errors eveError = Errors.forCode(unproxiedResponsesByUser.get(EVE).errorCode());
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

    List<Arguments> shouldEnforceAccessToTopics() {

        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.HEARTBEAT),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.HEARTBEAT))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " alice group request",
                                new HeartbeatEquivalence((short) (int) apiVersion, ALICE_GROUP_PREFIX)),
                        Arguments.argumentSet("api version " + apiVersion + " bob group request",
                                new HeartbeatEquivalence((short) (int) apiVersion, BOB_GROUP_PREFIX))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.HEARTBEAT.oldestVersion(), ApiKeys.HEARTBEAT.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.HEARTBEAT, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.HEARTBEAT, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<ConsumerGroupHeartbeatRequestData, ConsumerGroupHeartbeatResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
