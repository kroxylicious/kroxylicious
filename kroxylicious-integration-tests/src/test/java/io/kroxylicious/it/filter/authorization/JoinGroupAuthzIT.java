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
import org.apache.kafka.common.message.JoinGroupRequestData;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static java.util.stream.Stream.concat;
import static org.assertj.core.api.Assertions.assertThat;

public class JoinGroupAuthzIT extends AuthzIT {

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

    class JoinGroupEquivalence extends Equivalence<JoinGroupRequestData, JoinGroupResponseData> {

        private final String group;
        private final String groupPrefix;

        JoinGroupEquivalence(short apiVersion, String groupPrefix) {
            super(apiVersion);
            this.group = groupPrefix + "-" + UUID.randomUUID();
            this.groupPrefix = groupPrefix;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.JOIN_GROUP;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public JoinGroupRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            JoinGroupRequestData request = new JoinGroupRequestData();
            request.setGroupId(Objects.requireNonNull(group));
            request.setSessionTimeoutMs(10000);
            request.setMemberId("");
            request.setProtocolType("consumer");
            JoinGroupRequestData.JoinGroupRequestProtocol protocol = new JoinGroupRequestData.JoinGroupRequestProtocol();
            protocol.setName("range");
            request.protocols().add(protocol);
            if (apiVersion() >= 1) {
                request.setRebalanceTimeoutMs(10000);
            }
            if (apiVersion() >= 5) {
                request.setGroupInstanceId(group + "-instanceId");
            }
            if (apiVersion() >= 8) {
                request.setReason("joining on a whim");
            }
            return request;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode root) {
            clobberString(root, "leader");
            clobberString(root, "memberId");
            JsonNode brokers = root.path("members");
            for (var broker : brokers) {
                if (broker.isObject()) {
                    clobberString((ObjectNode) broker, "memberId");
                }
            }
            return prettyJsonString(root);
        }

        @Override
        public void prepareCluster(BaseClusterFixture cluster) {
            KafkaDriver driver = new KafkaDriver(cluster, cluster.authenticatedClient(AuthzIT.SUPER, SUPER_PASSWORD), AuthzIT.SUPER);
            driver.findCoordinator(FindCoordinatorRequest.CoordinatorType.GROUP, group);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, JoinGroupResponseData> unproxiedResponsesByUser) {
            Errors aliceError = Errors.forCode(unproxiedResponsesByUser.get(ALICE).errorCode());
            Errors bobError = Errors.forCode(unproxiedResponsesByUser.get(BOB).errorCode());
            Errors eveError = Errors.forCode(unproxiedResponsesByUser.get(EVE).errorCode());
            assertThat(eveError).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
            if (groupPrefix.equals(ALICE_GROUP_PREFIX)) {
                if (apiVersion() == 4) {
                    // since v4 a request with an unknown member id and no groupInstanceId responds with this error code
                    assertThat(aliceError).isEqualTo(Errors.MEMBER_ID_REQUIRED);
                }
                else {
                    assertThat(aliceError).isEqualTo(Errors.NONE);
                }
                assertThat(bobError).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
            }
            else if (groupPrefix.equals(BOB_GROUP_PREFIX)) {
                assertThat(aliceError).isEqualTo(Errors.GROUP_AUTHORIZATION_FAILED);
                if (apiVersion() == 4) {
                    // since v4 a request with an unknown member id and no groupInstanceId responds with this error code
                    assertThat(bobError).isEqualTo(Errors.MEMBER_ID_REQUIRED);
                }
                else {
                    assertThat(bobError).isEqualTo(Errors.NONE);
                }
            }
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {

        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.JOIN_GROUP),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.JOIN_GROUP))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " alice group request",
                                new JoinGroupEquivalence((short) (int) apiVersion, ALICE_GROUP_PREFIX)),
                        Arguments.argumentSet("api version " + apiVersion + " bob group request",
                                new JoinGroupEquivalence((short) (int) apiVersion, BOB_GROUP_PREFIX))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.JOIN_GROUP.oldestVersion(), ApiKeys.JOIN_GROUP.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.JOIN_GROUP, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.JOIN_GROUP, (short) apiVersion)));
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
