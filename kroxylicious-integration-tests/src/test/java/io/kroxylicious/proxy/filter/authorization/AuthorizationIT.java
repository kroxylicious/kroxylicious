/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.SaslPlainTermination;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class AuthorizationIT extends BaseIT {

    // 1. Spin a cluster with Users:
    // * Alice directly authorized for operation
    // * Bob indirectly authorized for operation (by implication)
    // * Eve not authorized for operation
    // 2. Do some prep (e.g. create a topic T, create a group G)
    // 3. Make a request for T as each of Alice, Bob and Eve. Record the response
    // 4. Assert visible side effects
    // 5. Tear down the cluster
    // 6. Spin a proxied cluster with proxy users authorised the same way
    // 7. Do the same prep (e.g. create a topic T, create a group G) (non proxied)
    // 8. Make a proxied request for T as each of Alice, Bob and Eve
    // 9. Assert that the responses are ==
    // 10. Assert no visible side effects

    public static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static Path rulesFile;
    private static String topicName = "topic";
    private static String nonExistingTopicCreateAllowed = "non-existing-topic-create-allowed";
    private static String nonExistingTopicCreateDenied = "non-existing-topic-create-denied";
    private static List<AclBinding> aclBindings;

    @BeforeAll
    static void beforeAll() throws IOException {
        // TODO need to add Carol who has Cluster.CREATE
        rulesFile = Files.createTempFile(AuthorizationIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
            version 1;
            import User from io.kroxylicious.proxy.internal.subject; // TODO This can't remain in the internal package!
            import TopicResource as Topic from io.kroxylicious.filter.authorization;
            allow User with name = "alice" to * Topic with name = "%s";
            allow User with name = "alice" to * Topic with name = "%s";
            allow User with name = "bob" to READ Topic with name = "%s";
            otherwise deny;
            """.formatted(topicName, nonExistingTopicCreateAllowed, topicName));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, nonExistingTopicCreateAllowed, PatternType.LITERAL),
                        new AccessControlEntry("User:alice", "*", AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                        new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW)));
    }

    static List<Arguments> metadata() {
        // The tuples
        List<Short> apiVersions = ApiKeys.METADATA.allVersions();
        boolean[] allowTopicCreations = { true, false };
        boolean[] clusterAuthz = { true, false };
        boolean[] topicAuthz = { true, false };
        List[] requestedTopics = {
                null,
                List.of(),
                List.of(
                        new MetadataRequestData.MetadataRequestTopic().setName(topicName),
                        new MetadataRequestData.MetadataRequestTopic().setName(nonExistingTopicCreateDenied),
                        new MetadataRequestData.MetadataRequestTopic().setName(nonExistingTopicCreateAllowed))
        };

        // Compute the n-fold Cartesian product of the tuples (except for pruning)
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            for (boolean allowAutoCreation : allowTopicCreations) {
                if (!allowAutoCreation && apiVersion < 4) {
                    // We have to prune some combinations because the RequestData will reject
                    // non-default values for fields which don't exist in a certain version
                    continue;
                }
                for (boolean includeClusterAuthz : clusterAuthz) {
                    if (apiVersion < 8 && includeClusterAuthz) {
                        continue;
                    }
                    if (apiVersion >= 11 && includeClusterAuthz) {
                        continue;
                    }
                    for (boolean includeTopicAuthz : topicAuthz) {
                        if (apiVersion < 8 && includeTopicAuthz) {
                            continue;
                        }
                        for (List<MetadataRequestData.MetadataRequestTopic> topics : requestedTopics) {
                            if (topics == null && apiVersion < 1) {
                                continue;
                            }
                            result.addAll(List.of(
                                    Arguments.of(apiVersion, allowAutoCreation, includeClusterAuthz, includeTopicAuthz, topics)));
                        }
                    }
                }
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void metadata(
                  short apiVersion,
                  boolean allowAutoCreation,
                  boolean includeClusterAuthz,
                  boolean includeTopicsAuthz,
                  List<MetadataRequestData.MetadataRequestTopic> topics,
                  @SaslMechanism(principals = {
                          @SaslMechanism.Principal(user = "super", password = "Super"),
                          @SaslMechanism.Principal(user = "alice", password = "Alice"),
                          @SaslMechanism.Principal(user = "bob", password = "Bob"),
                          @SaslMechanism.Principal(user = "eve", password = "Eve")
                  }) @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
                  // ANONYMOUS is the broker
                  @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:super") KafkaCluster unproxiedCluster,
                  KafkaCluster proxiedCluster) throws JsonProcessingException {

        String topicName = "topic";
        Map<String, String> passwords = Map.of(
                "alice", "Alice",
                "bob", "Bob",
                "eve", "Eve");

        prepCluster(unproxiedCluster, topicName, aclBindings);
        prepCluster(proxiedCluster, topicName, List.of());

        var unproxiedResponsesByUser = responsesByUser(apiVersion,
                allowAutoCreation,
                topics,
                includeClusterAuthz,
                includeTopicsAuthz,
                unproxiedCluster.getBootstrapServers(),
                passwords);
        // TODO assertions about side effects (topics created, or not)

        if (topics == null || !topics.isEmpty()) {
            // When topics are requested, we expect alice and bob to be able to see them
            JsonNode path = unproxiedResponsesByUser.get("alice").path("topics");
            assertThat(path.isArray()).isTrue();
            assertThat(path.size()).isGreaterThan(0);
            path = unproxiedResponsesByUser.get("bob").path("topics");
            assertThat(path.isArray()).isTrue();
            assertThat(path.size()).isGreaterThan(0);
        }

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
                    allowAutoCreation,
                    topics,
                    includeClusterAuthz,
                    includeTopicsAuthz,
                    tester.getBootstrapAddress(),
                    passwords);

            assertThat(clobberMap(proxiedResponsesByUser))
                    .isEqualTo(clobberMap(unproxiedResponsesByUser));
            // TODO assertions about side effects (topics created, or not)
        }
    }

    private static Map<String, ObjectNode> responsesByUser(short apiVersion,
                                                           boolean allowAutoCreation,
                                                           List<MetadataRequestData.MetadataRequestTopic> topics,
                                                           boolean includeClusterAuthz,
                                                           boolean includeTopicsAuthz,
                                                           String bootstrapServers,
                                                           Map<String, String> passwords) {
        var responsesByUser = new HashMap<String, ObjectNode>();
        for (var entry : passwords.entrySet()) {
            try (KafkaClient unproxiedClient = client(bootstrapServers)) {
                authenticate(unproxiedClient, entry.getKey(), entry.getValue());

                var resp = unproxiedClient.getSync(new Request(ApiKeys.METADATA, apiVersion, "test",
                        new MetadataRequestData()
                                .setAllowAutoTopicCreation(allowAutoCreation)
                                .setTopics(topics)
                                .setIncludeClusterAuthorizedOperations(includeClusterAuthz)
                                .setIncludeTopicAuthorizedOperations(includeTopicsAuthz)));

                var r = (MetadataResponseData) resp.payload().message();
                ObjectNode json = (ObjectNode) MetadataResponseDataJsonConverter.write(r, apiVersion);
                responsesByUser.put(entry.getKey(), json);
            }
        }
        return responsesByUser;
    }

    @NonNull
    private static Map<String, String> clobberMap(Map<String, ObjectNode> unproxiedResponsesByUser) {
        return unproxiedResponsesByUser.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> clobberMetadata(entry.getValue())));
    }

    private static String clobberMetadata(final ObjectNode root) {
        clobberUuid(root, "clusterId");
        JsonNode brokers = root.path("brokers");
        for (var broker : brokers) {
            if (broker.isObject()) {
                ((ObjectNode) broker).put("port", "CLOBBERED");
            }
        }

        var topics = sortArray(root, "topics", "name");
        for (var topics1 : topics) {
            if (topics1.isObject()) {
                clobberUuid((ObjectNode) topics1, "topicId");
            }
        }
        
        try {
            return MAPPER.writeValueAsString(root);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static JsonNode maybeClobberedUuid(JsonNode uuid) {
        if (uuid != null && uuid.isTextual()) {
            String text = uuid.asText();
            if (!Uuid.RESERVED.contains(Uuid.fromString(text))) {
                return TextNode.valueOf("CLOBBERED");
            }
            else {
                return uuid;
            }
        }
        return uuid;
    }

    private static void clobberUuid(ObjectNode root, String propertyName) {
        root.replace(propertyName, maybeClobberedUuid(root.get(propertyName)));
    }

    private static ArrayNode sortArray(ObjectNode root, String arrayProperty, String sortProperty) {
        JsonNode topics = root.path("topics");
        if (topics.isArray()) {
            var sortedTopics = topics.valueStream().sorted(Comparator.comparing(itemNode -> itemNode.get(sortProperty).textValue())).toList();
            root.putArray(arrayProperty).addAll(sortedTopics);
            return (ArrayNode) root.get(arrayProperty);
        }
        return null;
    }

    private static Uuid prepCluster(KafkaCluster unproxiedCluster,
                                    String topicName,
                                    List<AclBinding> bindings) {
        Uuid topicIdInUnproxiedCluster;
        try (var admin = AdminClient.create(unproxiedCluster.getKafkaClientConfiguration("super", "Super"))) {
            topicIdInUnproxiedCluster = admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)))
                    .topicId(topicName)
                    .toCompletionStage().toCompletableFuture().join();

            if (!bindings.isEmpty()) {
                admin.createAcls(bindings).all()
                        .toCompletionStage().toCompletableFuture().join();
            }
        }
        return topicIdInUnproxiedCluster;
    }

    private static void authenticate(KafkaClient client, String username, String password) {
        // For this test we don't really care what the authn mechanism is, so we use the simplest, plain
        // because we have to do the SASL dance ourselves via the very basic `KafkaClient`
        var handshakeResponse = (SaslHandshakeResponseData) client.getSync(new Request(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(), "test",
                new SaslHandshakeRequestData()
                        .setMechanism("PLAIN")))
                .payload().message();
        assertThat(Errors.forCode(handshakeResponse.errorCode())).isEqualTo(Errors.NONE);

        var authenticateResponse = (SaslAuthenticateResponseData) client.getSync(new Request(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(), "test",
                new SaslAuthenticateRequestData()
                        .setAuthBytes((username + "\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8))))
                .payload().message();
        assertThat(Errors.forCode(authenticateResponse.errorCode())).isEqualTo(Errors.NONE);
    }

    /**
     * @param bootstrapServers The cluster to connect to.
     * @return A KafkaClient connected to the given cluster.
     */
    @NonNull
    private static KafkaClient client(String bootstrapServers) {
        String[] hostPort = bootstrapServers.split(",")[0].split(":");
        return new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]));
    }
}
