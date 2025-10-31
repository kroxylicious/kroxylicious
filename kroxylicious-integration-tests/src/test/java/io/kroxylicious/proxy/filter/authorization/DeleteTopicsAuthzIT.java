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
import java.util.Objects;
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
import org.apache.kafka.common.message.DeleteTopicsResponseDataJsonConverter;
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

import static org.assertj.core.api.Assertions.assertThat;

public class DeleteTopicsAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TO_CREATE_TOPIC_NAME = "eve-topic";

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @BeforeAll
    void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                version 1;
                import User from io.kroxylicious.proxy.authentication;
                import TopicResource as Topic from io.kroxylicious.filter.authorization;
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
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthz, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthz, ALL_TOPIC_NAMES_IN_TEST, List.of());
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
        public DeleteTopicsRequestData requestData(String user, Map<String, Uuid> topicNameToId) {
            return requestTemplate.request(user, topicNameToId);
        }

        @Override
        public ObjectNode convertResponse(DeleteTopicsResponseData response) {
            return (ObjectNode) DeleteTopicsResponseDataJsonConverter.write(response, apiVersion());
        }

        @Override
        public String clobberResponse(ObjectNode jsonResponse) {
            var topics = sortArray(jsonResponse, "responses", "name");
            for (var topics1 : topics) {
                if (topics1.isObject()) {
                    clobberUuid((ObjectNode) topics1, "topicId");
                }
            }
            return prettyJsonString(jsonResponse);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            // Assuming the requests for alice and bob were successful we expect their topics to have been deleted
            // We never expect eve's topic to get deleted
            assertThat(topicListing(cluster)).isEqualTo(Set.of("eve-topic"));
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

    List<Arguments> test() {
        DeleteTopicsRequestData[] requests = {
                new DeleteTopicsRequestData()
                        .setTimeoutMs(60_000)
                        .setTopicNames(List.of("")),
                new DeleteTopicsRequestData()
                        .setTimeoutMs(60_000)
                        .setTopics(List.of(new DeleteTopicsRequestData.DeleteTopicState().setName(""))),
                new DeleteTopicsRequestData()
                        .setTimeoutMs(60_000)
                        .setTopics(List.of(new DeleteTopicsRequestData.DeleteTopicState().setTopicId(Uuid.randomUuid()))),
        };

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : ApiKeys.DELETE_TOPICS.allVersions()) {
            if (!AuthorizationFilter.isApiVersionSupported(ApiKeys.DELETE_TOPICS, apiVersion)) {
                result.add(
                        Arguments.of(new UnsupportedApiVersion<>(ApiKeys.DELETE_TOPICS, apiVersion)));
            }
            else {
                for (var request : requests) {
                    RequestTemplate<DeleteTopicsRequestData> requestTemplate = new RequestTemplate<>() {
                        @Override
                        public DeleteTopicsRequestData request(String user, Map<String, Uuid> topicNameToId) {
                            var result = request.duplicate();
                            String topicName = user + "-topic";
                            if (result.topicNames() != null) {
                                result.setTopicNames(result.topicNames().stream().map(name -> topicName).toList());
                            }
                            else if (result.topics() != null) {
                                result.setTopics(result.topics().stream().map(state -> {
                                    if (state.name() != null) {
                                        state.setName(topicName);
                                    }
                                    else if (state.topicId() != null) {
                                        state.setTopicId(Objects.requireNonNull(topicNameToId.get(topicName)));
                                    }
                                    else {
                                        throw new IllegalStateException();
                                    }
                                    return state;
                                }).toList());
                            }
                            else {
                                throw new IllegalStateException();
                            }
                            return result;
                        }
                    };

                    if (apiVersion <= 5) {
                        if (request.topicNames() != null && !request.topicNames().isEmpty()) {
                            result.add(
                                    Arguments.of(new DeleteTopicsEquivalence(apiVersion, requestTemplate)));
                        }
                    }
                    else {
                        if (request.topics() != null && !request.topics().isEmpty()) {
                            result.add(
                                    Arguments.of(new DeleteTopicsEquivalence(apiVersion, requestTemplate)));
                        }
                    }
                }
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void test(VersionSpecificVerification<DeleteTopicsRequestData, DeleteTopicsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(this.kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(this.kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
