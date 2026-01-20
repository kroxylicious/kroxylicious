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
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
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

import io.kroxylicious.filter.authorization.InitProducerIdEnforcement;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.Name;

class InitProducerIdAuthzIT extends AuthzIT {

    private static final String ALICE_TXN_ID = "alice-txn";
    private static final String BOB_TXN_ID = "bob-txn";
    private static final String EVE_TXN_ID = "eve-txn";
    private static final String NON_EXISTING_TXN_ID = "non-existing-txn";
    public static final List<String> ALL_TXN_IDS_IN_TEST = List.of(
            ALICE_TXN_ID,
            BOB_TXN_ID,
            EVE_TXN_ID,
            NON_EXISTING_TXN_ID);
    public static final List<String> ALL_TOPICS_IN_TEST = List.<String> of();
    public static final String TXN_COORDINATOR_OBSERVER = "txn-coordinator-observer";

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
                allow User with name = "alice" to * TxnId with name = "%s";
                allow User with name = "bob" to DESCRIBE TxnId with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TXN_ID, BOB_TXN_ID));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, ALICE_TXN_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, BOB_TXN_ID, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters(
                      @Name("kafkaClusterWithAuthz") @ClientConfig(name = ProducerConfig.TRANSACTIONAL_ID_CONFIG, value = TXN_COORDINATOR_OBSERVER) @ClientConfig(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") @ClientConfig(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") Producer kafkaClusterWithAuthzProducer,

                      @Name("kafkaClusterNoAuthz") @ClientConfig(name = ProducerConfig.TRANSACTIONAL_ID_CONFIG, value = TXN_COORDINATOR_OBSERVER) @ClientConfig(name = ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") @ClientConfig(name = ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, value = "org.apache.kafka.common.serialization.StringSerializer") Producer kafkaClusterNoAuthzProducer)
            throws InterruptedException, ExecutionException {
        System.out.println(kafkaClusterWithAuthzAdmin.describeFeatures().featureMetadata().get());
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

    class InitProducerIdEquivalence extends Equivalence<InitProducerIdRequestData, InitProducerIdResponseData> {

        private final RequestTemplate<InitProducerIdRequestData> requestTemplate;

        InitProducerIdEquivalence(
                                   short apiVersion,
                                   RequestTemplate<InitProducerIdRequestData> requestTemplate) {
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
            return ApiKeys.INIT_PRODUCER_ID;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public InitProducerIdRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestTemplate.request(user, clusterFixture);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            // initializing a PID does not on its own start a txn
            // => there's nothing observable via the admin client
        }

    }

    List<Arguments> shouldEnforceAccessToTransactionalIds() {
        // The tuples
        List<Short> apiVersions = ApiKeys.INIT_PRODUCER_ID.allVersions();

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            if (apiVersion >= InitProducerIdEnforcement.MIN_VERSION_SUPPORTING_2PC) {
                continue;
            }
            for (var txnId : ALL_TXN_IDS_IN_TEST) {
                result.add(
                        Arguments.of(new InitProducerIdEquivalence(apiVersion, new RequestTemplate<>() {
                            @Override
                            public InitProducerIdRequestData request(String user, BaseClusterFixture topicNameToId) {
                                return new InitProducerIdRequestData()
                                        .setTransactionalId(txnId);
                            }

                            @Override
                            public String toString() {
                                return txnId;
                            }
                        })));
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
