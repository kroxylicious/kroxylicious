/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.FindCoordinatorEnforcement;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static io.kroxylicious.it.filter.authorization.AbstractAuthzEquivalenceIT.prepCluster;
import static io.kroxylicious.it.filter.authorization.ClusterPrepUtils.deleteTopicsAndAcls;

class FindCoordinatorGroupAuthzIT extends AuthzIT {

    private static final String FOO_GROUP = "foo";
    private static final String BAR_GROUP = "bar";
    private static final String NON_EXISTENT_GROUP = "non-existent-group";
    public static final List<String> ALL_GROUPS_IN_TEST = List.of(
            FOO_GROUP,
            BAR_GROUP,
            NON_EXISTENT_GROUP);
    public static final List<String> ALL_TOPICS_IN_TEST = List.of();
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
                allow User with name = "%s" to * Group with name = "%s";
                allow User with name = "%s" to DESCRIBE Group with name = "%s";
                otherwise deny;
                """.formatted(
                ALICE, FOO_GROUP,
                BOB, BAR_GROUP));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, FOO_GROUP, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BAR_GROUP, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters(
                      @Name("kafkaClusterWithAuthz") @ClientConfig(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") @ClientConfig(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") Producer kafkaClusterWithAuthzProducer,

                      @Name("kafkaClusterNoAuthz") @ClientConfig(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") @ClientConfig(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") Producer kafkaClusterNoAuthzProducer)
            throws InterruptedException, ExecutionException {
        prepCluster(kafkaClusterWithAuthz, ALL_TOPICS_IN_TEST, aclBindings);
        prepCluster(kafkaClusterNoAuthz, ALL_TOPICS_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPICS_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPICS_IN_TEST, List.of());
    }

    class FindCoordinatorEquivalence extends Equivalence<FindCoordinatorRequestData, FindCoordinatorResponseData> {

        private final RequestTemplate<FindCoordinatorRequestData> requestTemplate;

        FindCoordinatorEquivalence(
                                   short apiVersion,
                                   RequestTemplate<FindCoordinatorRequestData> requestTemplate) {
            super(apiVersion);
            this.requestTemplate = requestTemplate;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "{" +
                    "apiVersion=" + apiVersion() +
                    ", requestTemplate=" + requestTemplate +
                    '}';
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.FIND_COORDINATOR;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public FindCoordinatorRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestTemplate.request(user, clusterFixture);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {

            if (jsonNodes.path("errorCode").isShort() && jsonNodes.path("errorCode").shortValue() == 0) {
                clobberInt(jsonNodes, "port", 1234);
            }
            JsonNode coordinators = jsonNodes.path("coordinators");
            if (coordinators.isArray()) {
                for (var coord : coordinators) {
                    if (coord.path("errorCode").isShort() && coord.path("errorCode").shortValue() == 0) {
                        clobberInt((ObjectNode) coord, "port", 1234);
                    }
                }
                sortArray(jsonNodes, "coordinators", "key");
            }
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            // There are not visible side effects from a find coordinators call
        }

    }

    List<Arguments> shouldEnforceAccessToGroups() {
        // The tuples
        List<Short> apiVersions = ApiKeys.FIND_COORDINATOR.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (apiVersion < FindCoordinatorEnforcement.MIN_API_VERSION_WITH_KEY) {
                for (var group : ALL_GROUPS_IN_TEST) {
                    result.add(
                            Arguments.of(new FindCoordinatorEquivalence(apiVersion, new RequestTemplate<>() {
                                @Override
                                public FindCoordinatorRequestData request(String user, BaseClusterFixture topicNameToId) {
                                    // key type is implicitly GROUP for v0
                                    return new FindCoordinatorRequestData().setKey(group);
                                }

                                @Override
                                public String toString() {
                                    return group;
                                }
                            })));
                }
            }
            else if (apiVersion >= FindCoordinatorEnforcement.MIN_API_VERSION_USING_BATCHING) {
                result.add(
                        Arguments.of(new FindCoordinatorEquivalence(apiVersion, new RequestTemplate<>() {
                            @Override
                            public FindCoordinatorRequestData request(String user, BaseClusterFixture baseClusterFixture) {
                                return new FindCoordinatorRequestData()
                                        .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                                        .setCoordinatorKeys(ALL_GROUPS_IN_TEST);
                            }

                            @Override
                            public String toString() {
                                return ALL_GROUPS_IN_TEST.toString();
                            }
                        })));
            }
            else {
                for (var group : ALL_GROUPS_IN_TEST) {
                    result.add(
                            Arguments.of(new FindCoordinatorEquivalence(apiVersion, new RequestTemplate<>() {
                                @Override
                                public FindCoordinatorRequestData request(String user, BaseClusterFixture topicNameToId) {
                                    return new FindCoordinatorRequestData()
                                            .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                                            .setKey(group);
                                }

                                @Override
                                public String toString() {
                                    return group;
                                }
                            })));
                }
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToGroups(VersionSpecificVerification<CreatePartitionsRequestData, CreatePartitionsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
