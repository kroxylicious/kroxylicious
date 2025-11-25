/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
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

public class DeleteTopicsAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TO_CREATE_TOPIC_NAME = "eve-topic";

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
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DELETE Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TOPIC_NAME, BOB_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DELETE, AclPermissionType.ALLOW)));
    }

    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME,
            EVE_TO_CREATE_TOPIC_NAME);

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    private static class DeleteTopicsByIdTemplate implements RequestTemplate<DeleteTopicsRequestData> {
        private final boolean addUserTopic;
        private final boolean addUnknownTopic;

        DeleteTopicsByIdTemplate(boolean addUserTopic, boolean addUnknownTopic) {
            this.addUserTopic = addUserTopic;
            this.addUnknownTopic = addUnknownTopic;
        }

        @Override
        public DeleteTopicsRequestData request(String user, BaseClusterFixture clusterFixture) {

            DeleteTopicsRequestData request = new DeleteTopicsRequestData()
                    .setTimeoutMs(60_000);
            if (addUserTopic) {
                var state = new DeleteTopicsRequestData.DeleteTopicState()
                        .setTopicId(clusterFixture.topicIds().get(user + "-topic"));
                request.topics().add(state);
            }
            if (addUnknownTopic) {
                var state2 = new DeleteTopicsRequestData.DeleteTopicState()
                        .setTopicId(Uuid.randomUuid());
                request.topics().add(state2);
            }
            return request;
        }

        @Override
        public String toString() {
            return "delete by topicId:" + (addUserTopic ? " {topicId for name ${user}-topic}" : "") + (addUnknownTopic ? " {unknownTopicId}" : "");
        }
    }

    class DeleteTopicsEquivalence extends Equivalence<DeleteTopicsRequestData, DeleteTopicsResponseData> {

        private final RequestTemplate<DeleteTopicsRequestData> requestTemplate;

        DeleteTopicsEquivalence(
                                short apiVersion,
                                RequestTemplate<DeleteTopicsRequestData> requestTemplate) {
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
            return ApiKeys.DELETE_TOPICS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public DeleteTopicsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestTemplate.request(user, clusterFixture);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse) {
            jsonResponse.get("responses").forEach(response -> {
                var topicId = response.path("topicId").asText(null);
                if (topicId != null && !response.has("name")) {
                    for (var e : cluster.topicIds().entrySet()) {
                        if (e.getValue().toString().equals(topicId)) {
                            ((ObjectNode) response).put("name", e.getKey());
                        }
                    }
                }
            });
            var topics = sortArray(jsonResponse, "responses", "name", "errorMessage");
            for (var topics1 : topics) {
                if (topics1.isObject()) {
                    clobberUuid((ObjectNode) topics1, "topicId");
                }
            }
            return prettyJsonString(jsonResponse);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
        }

        @Override
        public Object observedVisibleSideEffects(BaseClusterFixture cluster) {
            return topicListing(cluster);
        }

        @Override
        public void assertUnproxiedResponses(Map<String, DeleteTopicsResponseData> unproxiedResponsesByUser) {
            // TODO
        }

        private Set<String> topicListing(BaseClusterFixture cluster) {
            try (var admin = Admin.create(cluster.backingCluster().getKafkaClientConfiguration(SUPER, "Super"))) {
                var topics = admin.describeTopics(ALL_TOPIC_NAMES_IN_TEST).topicNameValues();
                return topics.values().stream().map(
                        value -> value.toCompletionStage().toCompletableFuture())
                        .filter(fut -> {
                            try {
                                fut.join();
                                return true;
                            }
                            catch (CompletionException e) {
                                return false;
                            }
                        }).map(fut -> fut.join().name())
                        .collect(Collectors.toSet());
            }
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : ApiKeys.DELETE_TOPICS.allVersions()) {
            if (!AuthorizationFilter.isApiVersionSupported(ApiKeys.DELETE_TOPICS, apiVersion)) {
                result.add(
                        Arguments.of(new UnsupportedApiVersion<>(ApiKeys.DELETE_TOPICS, apiVersion)));
            }
            else {
                if (apiVersion <= 5) {
                    for (List<String> topicNames : List.of(List.of(ALICE + "-topic"), ALL_TOPIC_NAMES_IN_TEST)) {
                        RequestTemplate<DeleteTopicsRequestData> requestTemplate = new RequestTemplate<>() {
                            @Override
                            public DeleteTopicsRequestData request(String user, BaseClusterFixture clusterFixture) {
                                return new DeleteTopicsRequestData()
                                        .setTimeoutMs(60_000)
                                        .setTopicNames(topicNames);
                            }

                            @Override
                            public String toString() {
                                return "delete by name " + topicNames;
                            }
                        };

                        result.add(
                                Arguments.of(new DeleteTopicsEquivalence(apiVersion, requestTemplate)));

                    }
                }
                else { // using states...
                       // ... with topic names
                    for (List<String> topicNames : List.of(List.of(ALICE + "-topic"), ALL_TOPIC_NAMES_IN_TEST)) {
                        RequestTemplate<DeleteTopicsRequestData> requestTemplate = new RequestTemplate<>() {
                            @Override
                            public DeleteTopicsRequestData request(String user, BaseClusterFixture clusterFixture) {
                                return new DeleteTopicsRequestData()
                                        .setTimeoutMs(60_000)
                                        .setTopics(topicNames.stream()
                                                .map(topicName -> new DeleteTopicsRequestData.DeleteTopicState()
                                                        .setName(topicName))
                                                .toList());
                            }

                            @Override
                            public String toString() {
                                return "delete by state with name " + topicNames;
                            }
                        };
                        result.add(
                                Arguments.of(new DeleteTopicsEquivalence(apiVersion, requestTemplate)));
                    }

                    // ... with topic ids
                    result.add(
                            Arguments.of(new DeleteTopicsEquivalence(apiVersion, new DeleteTopicsByIdTemplate(true, true))));
                    result.add(
                            Arguments.of(new DeleteTopicsEquivalence(apiVersion, new DeleteTopicsByIdTemplate(true, false))));
                    result.add(
                            Arguments.of(new DeleteTopicsEquivalence(apiVersion, new DeleteTopicsByIdTemplate(false, true))));
                }
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<DeleteTopicsRequestData, DeleteTopicsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(this.kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(this.kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
