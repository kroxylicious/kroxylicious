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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class MetadataAuthzEquivalenceIT extends AbstractAuthzEquivalenceIT {

    private static final Uuid SENTINEL_TOPIC_ID = Uuid.randomUuid();
    private static final Uuid NON_EXISTENT_TOPIC_ID = Uuid.randomUuid();

    private Path rulesFile;
    private static final String TOPIC_NAME = "topic";
    private static final String NON_EXISTING_TOPIC_CREATE_ALLOWED = "non-existing-topic-create-allowed";
    private static final String NON_EXISTING_TOPIC_CREATE_DENIED = "non-existing-topic-create-denied";
    private static List<AclBinding> aclBindings;
    private Uuid topicIdInUnproxiedCluster;
    private Uuid topicIdInProxiedCluster;

    @SaslMechanism(principals = {
            @SaslMechanism.Principal(user = "super", password = "Super"),
            @SaslMechanism.Principal(user = "alice", password = "Alice"),
            @SaslMechanism.Principal(user = "bob", password = "Bob"),
            @SaslMechanism.Principal(user = "eve", password = "Eve")
    })
    @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    // ANONYMOUS is the broker
    @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:super")
    static KafkaCluster unproxiedCluster;
    static KafkaCluster proxiedCluster;

    private static final Map<String, String> PASSWORDS = Map.of(
            "alice", "Alice",
            "bob", "Bob",
            "eve", "Eve");

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(MetadataAuthzEquivalenceIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to READ Topic with name = "%s";
                otherwise deny;
                """.formatted(TOPIC_NAME, NON_EXISTING_TOPIC_CREATE_ALLOWED, TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, TOPIC_NAME, PatternType.LITERAL),
                new AccessControlEntry("User:alice", "*", AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, NON_EXISTING_TOPIC_CREATE_ALLOWED, PatternType.LITERAL),
                        new AccessControlEntry("User:alice", "*", AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:bob", "*", AclOperation.READ, AclPermissionType.ALLOW)));
    }

    @AfterAll
    void afterAll() throws IOException {
        Files.deleteIfExists(rulesFile);
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdInUnproxiedCluster = prepCluster(unproxiedCluster, TOPIC_NAME, aclBindings);
        this.topicIdInProxiedCluster = prepCluster(proxiedCluster, TOPIC_NAME, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(unproxiedCluster, List.of(TOPIC_NAME, NON_EXISTING_TOPIC_CREATE_ALLOWED, NON_EXISTING_TOPIC_CREATE_DENIED), aclBindings);
        deleteTopicsAndAcls(proxiedCluster, List.of(TOPIC_NAME, NON_EXISTING_TOPIC_CREATE_ALLOWED, NON_EXISTING_TOPIC_CREATE_DENIED), List.of());
    }

    private static Map<String, ObjectNode> responsesByUser(short apiVersion,
                                                           boolean allowAutoCreation,
                                                           List<MetadataRequestData.MetadataRequestTopic> topics,
                                                           boolean includeClusterAuthz,
                                                           boolean includeTopicsAuthz,
                                                           String bootstrapServers,
                                                           Uuid topicIdReplacement,
                                                           Map<String, String> passwords) {
        var responsesByUser = new HashMap<String, ObjectNode>();
        for (var entry : passwords.entrySet()) {
            try (KafkaClient client = client(bootstrapServers)) {
                authenticate(client, entry.getKey(), entry.getValue());

                if (topics != null) {
                    for (var topic : topics) {
                        if (topic.topicId().equals(SENTINEL_TOPIC_ID)) {
                            topic.setTopicId(topicIdReplacement)
                                    .setName(null);
                        }
                    }
                }

                var resp = client.getSync(new Request(ApiKeys.METADATA, apiVersion, "test",
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
        return mapValues(unproxiedResponsesByUser, MetadataAuthzEquivalenceIT::clobberMetadata);
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

        return prettyJsonString(root);
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
                        new MetadataRequestData.MetadataRequestTopic().setName(TOPIC_NAME),
                        new MetadataRequestData.MetadataRequestTopic().setName(NON_EXISTING_TOPIC_CREATE_DENIED),
                        new MetadataRequestData.MetadataRequestTopic().setName(NON_EXISTING_TOPIC_CREATE_ALLOWED)),
                List.of(
                        // this is just a placeholder, replaced by the real id of topicName
                        new MetadataRequestData.MetadataRequestTopic().setName(TOPIC_NAME).setTopicId(SENTINEL_TOPIC_ID)),
                List.of(
                        // this is just a placeholder, replaced by the real id of topicName
                        new MetadataRequestData.MetadataRequestTopic().setTopicId(SENTINEL_TOPIC_ID)),
                List.of(
                        new MetadataRequestData.MetadataRequestTopic().setTopicId(NON_EXISTENT_TOPIC_ID))
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
                            if (apiVersion < 12 && topics != null && topics.stream().anyMatch(t -> t.topicId() != null)) {
                                // version 10 added support for topic ids, but
                                // "Versions 10 and 11 should not use the topicId field or set topic name to null." (from JSON IDL)
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
                  List<MetadataRequestData.MetadataRequestTopic> topics) {

        var unproxiedResponsesByUser = responsesByUser(apiVersion,
                allowAutoCreation,
                duplicateList(topics),
                includeClusterAuthz,
                includeTopicsAuthz,
                unproxiedCluster.getBootstrapServers(),
                topicIdInUnproxiedCluster,
                PASSWORDS);

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
                .withConfig("userNameToPassword", PASSWORDS)
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
                    duplicateList(topics),
                    includeClusterAuthz,
                    includeTopicsAuthz,
                    tester.getBootstrapAddress(),
                    topicIdInProxiedCluster,
                    PASSWORDS);

            assertThat(clobberMap(proxiedResponsesByUser))
                    .isEqualTo(clobberMap(unproxiedResponsesByUser));
        }
    }
}