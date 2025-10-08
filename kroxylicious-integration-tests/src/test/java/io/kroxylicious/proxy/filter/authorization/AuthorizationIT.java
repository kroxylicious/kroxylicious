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
import java.util.Arrays;
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

    public static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    static Path rulesFile;

    @BeforeAll
    static void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(AuthorizationIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
            version 1;
            import User from io.kroxylicious.proxy.internal.subject; // TODO This can't remain in the internal package!
            import TopicResource as Topic from io.kroxylicious.filter.authorization;
            allow User with name = "alice" to * Topic with name = "topic";
            allow User with name = "bob" to READ Topic with name = "topic";
            otherwise deny;
            """);
    }

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

    static List<Arguments> metadata() {
        // The tuples
        List<Short> apiVersions = List.of(ApiKeys.METADATA.latestVersion()); //ApiKeys.METADATA.allVersions();
        boolean[] allowTopicCreations = { true, false };
        boolean[] clusterAuthz = { true, false };
        boolean[] topicAuthz = { true, false };
        List[] requestedTopics = {
                null,
                List.of(),
                List.of(
                        new MetadataRequestData.MetadataRequestTopic().setName("topic"),
                        new MetadataRequestData.MetadataRequestTopic().setName("non-existing-topic"))
        };

        // Compute the n-fold Cartesian product of the tuples
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : apiVersions) {
            for (boolean allowAutoCreation : allowTopicCreations) {
                if (!allowAutoCreation && apiVersion < 4) {
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

        prepCluster(unproxiedCluster, topicName,
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                        new AccessControlEntry("User:alice", "*", AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                        new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW)));

        prepCluster(proxiedCluster, topicName);

        var unproxiedResponsesByUser = responsesByUser(apiVersion,
                allowAutoCreation,
                topics,
                includeClusterAuthz,
                includeTopicsAuthz,
                unproxiedCluster.getBootstrapServers(),
                passwords);

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
            var proxiedResponsedByUser = responsesByUser(
                    apiVersion,
                    allowAutoCreation,
                    topics,
                    includeClusterAuthz,
                    includeTopicsAuthz,
                    tester.getBootstrapAddress(),
                    passwords);

            assertThat(clobberMap(proxiedResponsedByUser))
                    .isEqualTo(clobberMap(unproxiedResponsesByUser));
        }
    }

    @NonNull
    private static Map<String, String> clobberMap(Map<String, ObjectNode> unproxiedResponsesByUser) {
        return unproxiedResponsesByUser.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> clobber(entry.getValue())));
    }

    private static Map<String, ObjectNode> responsesByUser(short apiVersion,
                                                                     boolean allowAutoCreation,
                                                                     List<MetadataRequestData.MetadataRequestTopic> topics,
                                                                     boolean includeClusterAuthz,
                                                                     boolean includeTopicsAuthz,
                                                                     String bootstrapServers,
                                                                     Map<String, String> passwords) throws JsonProcessingException {
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
                assertThat(Errors.forCode(r.errorCode())).isEqualTo(Errors.NONE);

                ObjectNode json = (ObjectNode) MetadataResponseDataJsonConverter.write(r, apiVersion);

                responsesByUser.put(entry.getKey(), json);
            }
        }
        return responsesByUser;
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

    private static String clobber(ObjectNode json) {

        json.replace("clusterId", maybeClobberedUuid(json.get("clusterId")));
        ((ObjectNode) json.path("brokers").path(0)).put("port", "CLOBBERED");
        var topics1 = json.path("topics").path(0);
        if (topics1.isObject()) { // could be missingnode
            ((ObjectNode) topics1).replace("topicId", maybeClobberedUuid(json.get("topicId")));
        }
        try {
            return MAPPER.writeValueAsString(json);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static Uuid prepCluster(KafkaCluster unproxiedCluster,
                                    String topicName,
                                    AclBinding... bindings) {
        Uuid topicIdInUnproxiedCluster;
        try (var admin = AdminClient.create(unproxiedCluster.getKafkaClientConfiguration("super", "Super"))) {
            topicIdInUnproxiedCluster = admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1)))
                    .topicId(topicName)
                    .toCompletionStage().toCompletableFuture().join();

            if (bindings.length > 0) {
                admin.createAcls(Arrays.asList(bindings)).all()
                        .toCompletionStage().toCompletableFuture().join();
            }
        }
        return topicIdInUnproxiedCluster;
    }

    private static void authenticate(KafkaClient client, String username, String password) {
        // For this test we don't really care what the authn mechanism is, so we use the simplest, plain
        // because we have to do the SASL dance ourselves via the very basic `KafkaClient`
        var h = (SaslHandshakeResponseData) client.getSync(new Request(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(), "test",
                new SaslHandshakeRequestData()
                        .setMechanism("PLAIN")))
                .payload().message();
        assertThat(Errors.forCode(h.errorCode())).isEqualTo(Errors.NONE);
        var a = (SaslAuthenticateResponseData) client.getSync(new Request(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(), "test",
                new SaslAuthenticateRequestData()
                        .setAuthBytes((username + "\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8))))
                .payload().message();
        assertThat(Errors.forCode(a.errorCode())).isEqualTo(Errors.NONE);
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
