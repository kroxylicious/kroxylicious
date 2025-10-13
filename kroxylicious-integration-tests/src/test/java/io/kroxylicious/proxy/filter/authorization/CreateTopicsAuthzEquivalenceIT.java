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
import java.util.HashMap;
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
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.assertj.core.api.AbstractComparableAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.SaslPlainTermination;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class CreateTopicsAuthzEquivalenceIT extends AbstractAuthzEquivalenceIT {

    public static final String SUPER = "super";
    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String EVE = "eve";
    private static final String ALICE_TO_CREATE_TOPIC_NAME = "alice-new-topic";
    private static final String BOB_TO_CREATE_TOPIC_NAME = "bob-new-topic";
    private static final String EVE_TO_CREATE_TOPIC_NAME = "eve-new-topic";
    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TO_CREATE_TOPIC_NAME,
            BOB_TO_CREATE_TOPIC_NAME,
            EVE_TO_CREATE_TOPIC_NAME,
            EXISTING_TOPIC_NAME);

    private static Path rulesFile;

    private static List<AclBinding> aclBindings;
    private Uuid topicIdInUnproxiedCluster;
    private Uuid topicIdInProxiedCluster;

    @SaslMechanism(principals = {
            @SaslMechanism.Principal(user = SUPER, password = "Super"),
            @SaslMechanism.Principal(user = ALICE, password = "Alice"),
            @SaslMechanism.Principal(user = BOB, password = "Bob"),
            @SaslMechanism.Principal(user = EVE, password = "Eve")
    }) @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    // ANONYMOUS is the broker
    @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:super")
    static KafkaCluster unproxiedCluster;
    static KafkaCluster proxiedCluster;

    Map<String, String> passwords = Map.of(
            ALICE, "Alice",
            BOB, "Bob",
            EVE, "Eve");

    @BeforeAll
    static void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(CreateTopicsAuthzEquivalenceIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
                version 1;
                import User from io.kroxylicious.proxy.internal.subject; // TODO This can't remain in the internal package!
                import TopicResource as Topic from io.kroxylicious.filter.authorization;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to CREATE Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TO_CREATE_TOPIC_NAME, BOB_TO_CREATE_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TO_CREATE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TO_CREATE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.CREATE, AclPermissionType.ALLOW)));
    }

    @AfterAll
    static void afterAll() throws IOException {
        Files.deleteIfExists(rulesFile);
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdInUnproxiedCluster = prepCluster(unproxiedCluster, EXISTING_TOPIC_NAME, aclBindings);
        this.topicIdInProxiedCluster = prepCluster(proxiedCluster, EXISTING_TOPIC_NAME, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(unproxiedCluster, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(proxiedCluster, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @Nullable
    private static List<CreateTopicsRequestData.CreatableTopic> duplicateTopics(List<CreateTopicsRequestData.CreatableTopic> topics) {
        if (topics != null) {
            return topics.stream()
                    .map(CreateTopicsRequestData.CreatableTopic::duplicate)
                    .toList();
        }
        return null;
    }

    private Map<String, ObjectNode> responsesByUser(short apiVersion,
                                                           List<CreateTopicsRequestData.CreatableTopic> topics,
                                                           String bootstrapServers,
                                                           Uuid topicIdReplacement) {
        var responsesByUser = new HashMap<String, ObjectNode>();
        for (var entry : passwords.entrySet()) {
            try (KafkaClient client = client(bootstrapServers)) {
                String user = entry.getKey();
                String password = entry.getValue();
                authenticate(client, user, password);

                var b = new CreateTopicsRequestData.CreatableTopicCollection();
                for (var topic : topics) {
//                    var t = new CreateTopicsRequestData.CreatableTopic()
//                            .setName(user + "-new-topic")
//                            //.setConfigs(null) TODO understand how this works WRT topic configs
//                            .setNumPartitions(1)
//                            .setReplicationFactor((short) 1);
                    b.mustAdd(topic.duplicate());
                }
                CreateTopicsRequestData message = new CreateTopicsRequestData()
                        .setTopics(b);
                var resp = client.getSync(new Request(ApiKeys.CREATE_TOPICS, apiVersion, "test",
                                message));

                var r = (CreateTopicsResponseData) resp.payload().message();
                ObjectNode json = (ObjectNode) CreateTopicsResponseDataJsonConverter.write(r, apiVersion);
                responsesByUser.put(user, json);
            }
        }
        return responsesByUser;
    }

    private static AbstractComparableAssert<?, Errors> assertErrorCodeAtPointer(
            String user,
            ObjectNode root,
            JsonPointer errorPtr,
            Errors expectedErrorCode) {
        JsonNode node = root.at(errorPtr);
        assertThat(node.isMissingNode())
                .as("%s should have a result at %s, but node is missing", user, errorPtr)
                .isFalse();
        return assertThat(Errors.forCode(node.shortValue()))
                .as("%s should have result %s", user, expectedErrorCode)
                .isEqualTo(expectedErrorCode);
    }

    static List<Arguments> createTopics() {
        // The tuples
        List<Short> apiVersions = ApiKeys.CREATE_TOPICS.allVersions();

        List[] requestedTopics = {
                ALL_TOPIC_NAMES_IN_TEST.stream().map(name ->
                        new CreateTopicsRequestData.CreatableTopic()
                                .setName(name)
                                .setNumPartitions(1)
                                .setReplicationFactor((short) 1)
                ).toList()
        };

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {

                for (List<CreateTopicsRequestData.CreatableTopic> topics : requestedTopics) {
                    result.add(
                            Arguments.of(apiVersion, topics));
                }

        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void createTopics(
            short apiVersion,
            List<CreateTopicsRequestData.CreatableTopic> topics) {

        var unproxiedResponsesByUser = responsesByUser(apiVersion,
                duplicateTopics(topics),
                unproxiedCluster.getBootstrapServers(),
                topicIdInUnproxiedCluster);
        // assertions about responses
//        if (topics == null || !topics.isEmpty()) {
//            // Sanity test what we expect the Kafka reponse to look like
//            JsonPointer namePtr = JsonPointer.compile("/topics/0/name");
//            JsonPointer errorPtr;
//            if (toCreateTopicName.equals(unproxiedResponsesByUser.get(ALICE).at(namePtr).textValue())) {
//                errorPtr = JsonPointer.compile("/topics/0/errorCode");
//            }
//            else {
//                errorPtr = JsonPointer.compile("/topics/1/errorCode");
//            }
//            assertErrorCodeAtPointer(ALICE, unproxiedResponsesByUser.get(ALICE), errorPtr, Errors.NONE);
//            assertErrorCodeAtPointer(BOB, unproxiedResponsesByUser.get(BOB), errorPtr, Errors.NONE);
//            assertErrorCodeAtPointer(EVE, unproxiedResponsesByUser.get(EVE), errorPtr, Errors.TOPIC_AUTHORIZATION_FAILED);
//        }
        // assertions about side effects
        assertVisibleSideEffects(unproxiedCluster);

        NamedFilterDefinition saslTermination = new NamedFilterDefinitionBuilder(
                "authn",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword", passwords)
                .build();
        NamedFilterDefinition authorization = new NamedFilterDefinitionBuilder(
                "authz",
                Authorization.class.getName())
                .withConfig("authorizer", AclAuthorizerService.class.getName(),
                        "authorizerConfig", Map.of("aclFile", rulesFile.toFile().getAbsolutePath()))
                .build();
        var config = proxy(proxiedCluster)
                .addToFilterDefinitions(saslTermination, authorization)
                .addToDefaultFilters(saslTermination.name(), authorization.name());

        try (var tester = kroxyliciousTester(config)) {

            var proxiedResponsesByUser = responsesByUser(
                    apiVersion,
                    duplicateTopics(topics),
                    tester.getBootstrapAddress(),
                    topicIdInProxiedCluster);

            // assert the responses from the proxied cluster at the same as from the unproxied cluster
            // (modulo clobbbering things like UUIDs which will be unavoidably different)
            assertThat(mapValues(proxiedResponsesByUser,
                    CreateTopicsAuthzEquivalenceIT::clobberResponse))
                    .as("Expect equivalent response to an unproxied Kafka cluster with the equivalent AuthZ")
                    .isEqualTo(mapValues(unproxiedResponsesByUser,
                            CreateTopicsAuthzEquivalenceIT::clobberResponse));
            // assertions about side effects
            assertVisibleSideEffects(proxiedCluster);
        }
    }

    private static String clobberResponse(ObjectNode jsonNodes) {
        var topics = sortArray(jsonNodes, "topics", "name");
        for (var topics1 : topics) {
            if (topics1.isObject()) {
                clobberUuid((ObjectNode) topics1, "topicId");
            }
        }
        return AbstractAuthzEquivalenceIT.prettyJsonString(jsonNodes);
    }

    private void assertVisibleSideEffects(KafkaCluster cluster) {
        assertThat(topicListing(cluster))
                .isEqualTo(Set.of(
                        "bob-new-topic", "alice-new-topic", "existing-topic"
                ));
    }

    private Set<String> topicListing(KafkaCluster cluster) {
        try (var admin = Admin.create(cluster.getKafkaClientConfiguration(SUPER, "Super"))) {
            var topics = admin.describeTopics(ALL_TOPIC_NAMES_IN_TEST).topicNameValues();
            return topics.values().stream().map(
                    value ->
                        value.toCompletionStage().toCompletableFuture())
                    .filter(fut -> {
                        try {
                            var desc = fut.join();
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