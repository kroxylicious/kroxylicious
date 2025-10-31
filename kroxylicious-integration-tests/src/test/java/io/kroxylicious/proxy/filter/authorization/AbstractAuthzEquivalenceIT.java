/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.assertj.core.api.AbstractComparableAssert;
import org.junit.jupiter.api.TestInstance;

import com.fasterxml.jackson.core.JsonPointer;
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
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.SaslPlainTermination;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A base class for "authorization equivalence" tests, which all follow the same basic pattern:
 * <ol>
 *     <li>
 *     Spin up two clusters. An unproxied one (from which we'll record how pure-Kafka AuthZ works).
 *     And a proxied one where the AuthZ will be done only in the proxy.
 *     </li>
 *     <li>In each cluster: Do identical preparation  (e.g. create a topic T, create a group G)</li>
 *     <li>In each cluster: Make a request for T as each of some common set of Users (e.g. Alice, Bob and Eve).</li>
 *     <li>Assert that the response from each cluster are "the same" (modulo things such as UUIDs which are not expected to be the same).</li>
 *     <li>In each cluster: Assert the same visible side effects (e.g. was a topic created, or were records appended)</li>
 * </ol>
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AbstractAuthzEquivalenceIT extends BaseIT {
    protected static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    protected static <P> Map<String, String> mapValues(Map<String, P> responsesByUser,
                                                       Function<P, String> valueMapper) {
        return responsesByUser.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> valueMapper.apply(entry.getValue())));
    }

    protected static ArrayNode sortArray(ObjectNode root, String arrayProperty, String sortProperty) {
        JsonNode topics = root.path(arrayProperty);
        if (topics.isArray()) {
            var sortedTopics = topics.valueStream().sorted(
                    Comparator.comparing(itemNode -> itemNode.get(sortProperty).textValue(),
                            Comparator.nullsFirst((String x, String y) -> x.compareTo(y))))
                    .toList();
            root.putArray(arrayProperty).addAll(sortedTopics);
            return (ArrayNode) root.get(arrayProperty);
        }
        return null;
    }

    protected static String prettyJsonString(final ObjectNode root) {
        try {
            return MAPPER.writeValueAsString(root);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected static JsonNode maybeClobberedUuid(JsonNode uuid) {
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

    protected static void clobberUuid(ObjectNode root, String propertyName) {
        root.replace(propertyName, maybeClobberedUuid(root.get(propertyName)));
    }

    protected static Uuid prepCluster(KafkaCluster unproxiedCluster,
                                      String topicName,
                                      List<AclBinding> bindings) {
        return prepCluster(unproxiedCluster, List.of(topicName), bindings)
                .get(topicName);
    }

    protected static Map<String, Uuid> prepCluster(KafkaCluster unproxiedCluster,
                                                   List<String> topicNames,
                                                   List<AclBinding> bindings) {
        Map<String, Uuid> result;
        try (var admin = AdminClient.create(unproxiedCluster.getKafkaClientConfiguration("super", "Super"))) {
            var res = admin.createTopics(topicNames.stream().map(topicName -> new NewTopic(topicName, 1, (short) 1)).toList());
            res.all().toCompletionStage().toCompletableFuture().join();
            result = topicNames.stream().collect(Collectors.toMap(Function.identity(), topicName -> {
                try {
                    return res.topicId(topicName).get();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }));

            if (!bindings.isEmpty()) {
                admin.createAcls(bindings).all()
                        .toCompletionStage().toCompletableFuture().join();
            }
        }
        return result;
    }

    protected static void deleteTopicsAndAcls(KafkaCluster unproxiedCluster,
                                              List<String> topicNames,
                                              List<AclBinding> bindings) {

        try (var admin = AdminClient.create(unproxiedCluster.getKafkaClientConfiguration("super", "Super"))) {
            try {
                admin.deleteTopics(TopicCollection.ofTopicNames(topicNames))
                        .all().toCompletionStage().toCompletableFuture().join();
            }
            catch (CompletionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                    throw e;
                }
            }

            if (!bindings.isEmpty()) {
                var filters = bindings.stream().map(AclBinding::toFilter).toList();
                admin.deleteAcls(filters).all()
                        .toCompletionStage().toCompletableFuture().join();
            }
        }
    }

    /**
     * @param bootstrapServers The cluster to connect to.
     * @return A KafkaClient connected to the given cluster.
     */
    protected static KafkaClient client(String bootstrapServers) {
        String[] hostPort = bootstrapServers.split(",")[0].split(":");
        return new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]));
    }

    protected static void authenticate(KafkaClient client, String username, String password) {
        // For this test we don't really care what the authn mechanism is, so we use the simplest, plain
        // because we have to do the SASL dance ourselves via the very basic `KafkaClient`
        var handshakeResponse = (SaslHandshakeResponseData) client.getSync(new Request(ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_HANDSHAKE.latestVersion(),
                "test",
                new SaslHandshakeRequestData()
                        .setMechanism("PLAIN")))
                .payload().message();
        assertThat(Errors.forCode(handshakeResponse.errorCode())).isEqualTo(Errors.NONE);

        byte[] bytes = (username + "\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8);
        var authenticateResponse = (SaslAuthenticateResponseData) client.getSync(new Request(ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                "test",
                new SaslAuthenticateRequestData()
                        .setAuthBytes(bytes)))
                .payload().message();
        assertThat(Errors.forCode(authenticateResponse.errorCode())).isEqualTo(Errors.NONE);
    }

    static AbstractComparableAssert<?, Errors> assertErrorCodeAtPointer(
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

    @SuppressWarnings("unchecked")
    static <M extends Message> @Nullable List<M> duplicateList(@Nullable List<M> topics) {
        if (topics != null) {
            return topics.stream()
                    .map(m -> (M) m.duplicate())
                    .toList();
        }
        return null;
    }

    public record Fixture(Map<String, String> passwords,
                          KafkaCluster unproxiedCluster,
                          Map<String, Uuid> topicIdsInUnproxiedCluster,
                          KafkaCluster proxiedCluster,
                          Map<String, Uuid> topicIdsInProxiedCluster,
                          Path rulesFile) {

    }

    interface RequestGen<Q extends ApiMessage, P extends ApiMessage> {
        ApiKeys apiKey();

        short apiVersion();

        Q request(String user, Map<String, Uuid> topicNameToId);

        ObjectNode convertResponse(P response);
    }

    interface Equivalence<Q extends ApiMessage, P extends ApiMessage> extends RequestGen<Q, P> {

        String clobberResponse(ObjectNode jsonResponse);

        void assertVisibleSideEffects(KafkaCluster cluster);
    }

    interface Unsupported<Q extends ApiMessage, P extends ApiMessage> extends RequestGen<Q, P> {

    }

    @NonNull
    protected ConfigurationBuilder proxyConfig(Fixture fixture) {
        NamedFilterDefinition saslTermination = new NamedFilterDefinitionBuilder(
                "authn",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword", fixture.passwords())
                .build();
        NamedFilterDefinition authorization = new NamedFilterDefinitionBuilder(
                "authz",
                Authorization.class.getName())
                .withConfig("authorizer", AclAuthorizerService.class.getName(),
                        "authorizerConfig", Map.of("aclFile", fixture.rulesFile().toFile().getAbsolutePath()))
                .build();
        var config = proxy(fixture.proxiedCluster())
                .addToFilterDefinitions(saslTermination, authorization)
                .addToDefaultFilters(saslTermination.name(), authorization.name());
        return config;
    }

    protected <Q extends ApiMessage, P extends ApiMessage> Map<String, P> responsesByUser(Map<String, String> passwords,
                                                                                          RequestGen<Q, P> scenario,
                                                                                          String bootstrapServers,
                                                                                          Map<String, Uuid> nameToId) {
        var responsesByUser = new HashMap<String, P>();
        for (var entry : passwords.entrySet()) {
            try (KafkaClient client = client(bootstrapServers)) {
                String user = entry.getKey();
                String password = entry.getValue();
                authenticate(client, user, password);

                var resp = client.getSync(new Request(scenario.apiKey(), scenario.apiVersion(), "test",
                        (Q) scenario.request(user, nameToId).duplicate()));

                var r = (P) resp.payload().message();
                responsesByUser.put(user, r);
            }
        }
        return responsesByUser;
    }

    protected <Q extends ApiMessage, P extends ApiMessage> void testApiEqivalence(Fixture fixture, Equivalence<Q, P> scenario) {
        short apiVersion = scenario.apiVersion();

        var unproxiedResponsesByUser = responsesByUser(
                fixture.passwords(),
                scenario,
                fixture.unproxiedCluster().getBootstrapServers(),
                fixture.topicIdsInUnproxiedCluster());
        // assertions about responses
        // if (topics == null || !topics.isEmpty()) {
        // // Sanity test what we expect the Kafka reponse to look like
        // JsonPointer namePtr = JsonPointer.compile("/topics/0/name");
        // JsonPointer errorPtr;
        // if (toCreateTopicName.equals(unproxiedResponsesByUser.get(ALICE).at(namePtr).textValue())) {
        // errorPtr = JsonPointer.compile("/topics/0/errorCode");
        // }
        // else {
        // errorPtr = JsonPointer.compile("/topics/1/errorCode");
        // }
        // assertErrorCodeAtPointer(ALICE, unproxiedResponsesByUser.get(ALICE), errorPtr, Errors.NONE);
        // assertErrorCodeAtPointer(BOB, unproxiedResponsesByUser.get(BOB), errorPtr, Errors.NONE);
        // assertErrorCodeAtPointer(EVE, unproxiedResponsesByUser.get(EVE), errorPtr, Errors.TOPIC_AUTHORIZATION_FAILED);
        // }

        // scenario.assertUnproxiedResponses(unproxiedResponsesByUser);
        // assertions about side effects
        scenario.assertVisibleSideEffects(fixture.unproxiedCluster());

        try (var tester = kroxyliciousTester(proxyConfig(fixture))) {

            var proxiedResponsesByUser = responsesByUser(
                    fixture.passwords(),
                    scenario,
                    tester.getBootstrapAddress(),
                    fixture.topicIdsInProxiedCluster());

            // assert the responses from the proxied cluster at the same as from the unproxied cluster
            // (modulo clobbbering things like UUIDs which will be unavoidably different)
            Function<P, String> convertAndClobberUserResponse = ((Function<P, ObjectNode>) scenario::convertResponse)
                    .andThen(scenario::clobberResponse);
            assertThat(mapValues(proxiedResponsesByUser,
                    convertAndClobberUserResponse))
                    .as("Expect equivalent response to an unproxied Kafka cluster with the equivalent AuthZ")
                    .isEqualTo(mapValues(unproxiedResponsesByUser,
                            convertAndClobberUserResponse));
            // assertions about side effects
            scenario.assertVisibleSideEffects(fixture.proxiedCluster());
        }
    }

    protected <Q extends ApiMessage, P extends ApiMessage> void testUnsupportedVersion(Fixture fixture,
                                                                                       Unsupported<Q, P> unsupported) {
        try (var tester = kroxyliciousTester(proxyConfig(fixture))) {
            var proxiedResponsesByUser = responsesByUser(fixture.passwords,
                    unsupported,
                    tester.getBootstrapAddress(),
                    fixture.topicIdsInProxiedCluster);

            for (String user : fixture.passwords().keySet()) {
                ObjectNode jsonNodes = unsupported.convertResponse(proxiedResponsesByUser.get(user));
                for (var topicNode : jsonNodes.path("topics")) {
                    assertThat(Errors.forCode(topicNode.get("errorCode").shortValue()))
                            .isEqualTo(Errors.UNSUPPORTED_VERSION);
                }
            }
        }
    }

}
