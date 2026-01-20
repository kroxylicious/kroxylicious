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

class FindCoordinatorAuthzIT extends AuthzIT {

    private static final String FOO_TXN_ID = "foo";
    private static final String BAR_TXN_ID = "bar";
    private static final String NON_EXISTING_TXN_ID = "non-existing-txn";
    public static final List<String> ALL_TXN_IDS_IN_TEST = List.of(
            FOO_TXN_ID,
            BAR_TXN_ID,
            NON_EXISTING_TXN_ID);
    public static final List<String> ALL_TOPICS_IN_TEST = List.<String> of();
    public static final String TXN_COORDINATOR_OBSERVER = "x";

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TransactionalIdResource as TxnId;
                allow User with name = "%s" to * TxnId with name = "%s";
                allow User with name = "%s" to DESCRIBE TxnId with name = "%s";
                otherwise deny;
                """.formatted(
                ALICE, FOO_TXN_ID,
                BOB, BAR_TXN_ID));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, FOO_TXN_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BAR_TXN_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters(
                      @Name("kafkaClusterWithAuthz") @ClientConfig(name = ProducerConfig.TRANSACTIONAL_ID_CONFIG, value = TXN_COORDINATOR_OBSERVER) @ClientConfig(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") @ClientConfig(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") Producer kafkaClusterWithAuthzProducer,

                      @Name("kafkaClusterNoAuthz") @ClientConfig(name = ProducerConfig.TRANSACTIONAL_ID_CONFIG, value = TXN_COORDINATOR_OBSERVER) @ClientConfig(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") @ClientConfig(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") Producer kafkaClusterNoAuthzProducer)
            throws InterruptedException, ExecutionException {
        prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPICS_IN_TEST, aclBindings);
        prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPICS_IN_TEST, List.of());
        ensureCoordinators(kafkaClusterWithAuthzProducer, TXN_COORDINATOR_OBSERVER, kafkaClusterWithAuthzAdmin);
        ensureCoordinators(kafkaClusterNoAuthzProducer, TXN_COORDINATOR_OBSERVER, kafkaClusterNoAuthzAdmin);
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

    List<Arguments> shouldEnforceAccessToTransactionalIds() {
        // The tuples
        List<Short> apiVersions = ApiKeys.FIND_COORDINATOR.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (apiVersion < FindCoordinatorEnforcement.MIN_API_VERSION_WITH_KEY) {
                // The API originally only supported group ids
                continue;
            }
            if (apiVersion >= FindCoordinatorEnforcement.MIN_API_VERSION_USING_BATCHING) {
                result.add(
                        Arguments.of(new FindCoordinatorEquivalence(apiVersion, new RequestTemplate<>() {
                            @Override
                            public FindCoordinatorRequestData request(String user, BaseClusterFixture baseClusterFixture) {
                                return new FindCoordinatorRequestData()
                                        .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                        .setCoordinatorKeys(ALL_TXN_IDS_IN_TEST);
                            }

                            @Override
                            public String toString() {
                                return ALL_TXN_IDS_IN_TEST.toString();
                            }
                        })));
            }
            else {
                for (var txnId : ALL_TXN_IDS_IN_TEST) {
                    result.add(
                            Arguments.of(new FindCoordinatorEquivalence(apiVersion, new RequestTemplate<FindCoordinatorRequestData>() {
                                @Override
                                public FindCoordinatorRequestData request(String user, BaseClusterFixture topicNameToId) {
                                    return new FindCoordinatorRequestData()
                                            .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                            .setKey(txnId);
                                }

                                @Override
                                public String toString() {
                                    return txnId;
                                }
                            })));
                }
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTransactionalIds(VersionSpecificVerification<CreatePartitionsRequestData, CreatePartitionsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
