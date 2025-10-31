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
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
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
import org.junit.jupiter.api.extension.ExtendWith;

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
import io.kroxylicious.test.RequestFactory;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * <p>A base class for integration tests covering some subset of the protocol
 * surface over which authorization is being tested.
 * In essence, we want tests over all combinations of:</p>
 * <ul>
 *     <li>Users who are, and are not, authorized to do the thing.
 *     Generally tests use Alice and Bob as the principals who are authorized
 *     for the API under test, and Eve as the principal who is not authorized.
 *     In addition, a Super user is used to set up and tear down any resources
 *     which the test requires.</li>
 *     <li>API key</li>
 *     <li>API version of that key</li>
 *     <li>Request "shape" (e.g. are topic names or ids being used?,
 *     or can some version of the request do some extra thing which requires additional testing,
 *     like returning authorized operations?). In general this means we can require multiple different requests
 *     for each user at a given API version.</li>
 * </ul>
 * <p>See also {@link AuthzFailsClosedIT} which is used to cover all the API keys and versions which the
 * {@link io.kroxylicious.filter.authorization.AuthorizationFilter} does not support.</p>
 *
 * <p>Note that this base class is {@code @TestInstance(TestInstance.Lifecycle.PER_CLASS)}, so that the {@code @BeforeAll},
 * {@code @AfterAll} and methods named by {@code @MethodSource} are <strong>not {@code static}</strong>.
 * This behaviour is inherited by subclasses.</p>
 */
@ExtendWith(KafkaClusterExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AuthzIT extends BaseIT {

    protected static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    public static final String SUPER = "super";
    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String EVE = "eve";

    public static final Map<String, String> PASSWORDS = Map.of(
            ALICE, "Alice",
            BOB, "Bob",
            EVE, "Eve");

    @SaslMechanism(principals = {
            @SaslMechanism.Principal(user = SUPER, password = "Super"),
            @SaslMechanism.Principal(user = ALICE, password = "Alice"),
            @SaslMechanism.Principal(user = BOB, password = "Bob"),
            @SaslMechanism.Principal(user = EVE, password = "Eve")
    })
    @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    // ANONYMOUS is the broker
    @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:super")
    static KafkaCluster kafkaClusterWithAuthz;
    static KafkaCluster kafkaClusterNoAuthz;

    Map<String, Uuid> topicIdsInUnproxiedCluster;
    Map<String, Uuid> topicIdsInProxiedCluster;

    /**
     * A version-specific test scenario.
     * @param <Q> The type of request.
     * @param <S> The type of response.
     */
    public interface VersionSpecificVerification<Q extends ApiMessage, S extends ApiMessage> {

        ApiKeys apiKey();

        short apiVersion();

        void verifyBehaviour(ReferenceCluster referenceCluster, ProxiedCluster proxiedCluster);

        Map<String, String> passwords();

        Q requestData(String user, Map<String, Uuid> topicNameToId);

        ObjectNode convertResponse(S response);

        default Stream<Errors> errors(S response) {
            return convertResponse(response).findValues("errorCode").stream().map(node -> {
                if (node.isShort()) {
                    return Errors.forCode(node.shortValue());
                }
                throw new IllegalStateException("Node called errorCode did not have a short value");
            });
        }

        default Map<String, Request> requests(Map<String, Uuid> topicsIds) {
            return Map.of(
                    ALICE, newRequest(requestData(ALICE, topicsIds)),
                    BOB, newRequest(requestData(BOB, topicsIds)),
                    EVE, newRequest(requestData(EVE, topicsIds)));
        }

        default Request newRequest(ApiMessage aliceRequest) {
            return new Request(ApiKeys.forId(aliceRequest.apiKey()), apiVersion(), "test", aliceRequest);
        }
    }

    /**
     * An abstraction for creating requests which depend on factors not known until just before the request is to be made.
     *
     * While we want to specify the general "shape" of a request to be defined in the test method parameter supplier
     * (i.e. the thing named in the {@code @MethodSource}), an actual request with that shape can
     * depend on things, like topic ids, which are specific to the cluster (which might be a ReferenceCluster or a ProxiedCluster).
     * This interface allow decoupling this cluster-dependence from the point where we define a shape of a test request.
     * @param <Q> The type of the request.
     */
    interface RequestTemplate<Q> {
        Q request(String user, Map<String, Uuid> topicNameToId);
    }

    /**
     * A version-specific test of "Kafka equivalence" of authorization
     * @param <Q> The type of request.
     * @param <S> The type of response.
     */
    public abstract class Equivalence<Q extends ApiMessage, S extends ApiMessage> implements VersionSpecificVerification<Q, S> {

        private final short apiVersion;

        Equivalence(short apiVersion) {
            this.apiVersion = apiVersion;
        }

        @Override
        public short apiVersion() {
            return apiVersion;
        }

        public abstract String clobberResponse(ObjectNode jsonResponse);

        public abstract void assertVisibleSideEffects(BaseClusterFixture cluster);

        public abstract void assertUnproxiedResponses(Map<String, S> unproxiedResponsesByUser);

        @Override
        public void verifyBehaviour(ReferenceCluster referenceCluster, ProxiedCluster proxiedCluster) {
            verifyApiEqivalence(
                    referenceCluster,
                    proxiedCluster,
                    this);
        }
    }

    /**
     * A test that requests made using a particular API (key, version)-pair, yields a Errors.UNSUPPORTED_VERSION response.
     * @param <Q> The type of request.
     * @param <S> The type of response.
     */
    public class UnsupportedApiVersion<Q extends ApiMessage, S extends ApiMessage> implements VersionSpecificVerification<Q, S> {

        private final ApiKeys apiKey;
        private final short apiVersion;

        UnsupportedApiVersion(ApiKeys apiKey, short apiVersion) {
            this.apiKey = apiKey;
            this.apiVersion = apiVersion;
        }

        @Override
        public String toString() {
            return "GenericUnsup[" +
                    "apiKey=" + apiKey + ", " +
                    "apiVersion=" + apiVersion + ']';
        }

        @Override
        public Q requestData(String user, Map topicNameToId) {
            return (Q) RequestFactory.apiMessageFor(apiKey(), apiVersion()).apiMessage();
        }

        @Override
        public ApiKeys apiKey() {
            return apiKey;
        }

        @Override
        public short apiVersion() {
            return apiVersion;
        }

        @Override
        public Map<String, String> passwords() {
            return Map.of(ALICE, "Alice");
        }

        @Override
        public ObjectNode convertResponse(ApiMessage response) {
            try {
                var converter = Class.forName(response.getClass().getName() + "JsonConverter");
                var method = converter.getDeclaredMethod("write", response.getClass(), Short.TYPE);
                return (ObjectNode) method.invoke(null, response, apiVersion());
            }
            catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Map<String, Request> requests(Map<String, Uuid> topicsIds) {
            return Map.of(
                    ALICE,
                    new Request(apiKey(), apiVersion(), "test",
                            RequestFactory.apiMessageFor(apiKey(), apiVersion()).apiMessage()));
        }

        @Override
        public void verifyBehaviour(ReferenceCluster referenceCluster, ProxiedCluster proxiedCluster) {
            verifyUnsupportedVersion(proxiedCluster, this);
        }

    }

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

    protected static Map<String, Uuid> prepCluster(KafkaCluster unproxiedCluster,
                                                   String topicName,
                                                   List<AclBinding> bindings) {
        return prepCluster(unproxiedCluster, List.of(topicName), bindings);
    }

    protected static Map<String, Uuid> prepCluster(KafkaCluster unproxiedCluster,
                                                   List<String> topicNames,
                                                   List<AclBinding> bindings) {
        Map<String, Uuid> result;
        try (var admin = AdminClient.create(unproxiedCluster.getKafkaClientConfiguration(SUPER, "Super"))) {
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

        try (var admin = AdminClient.create(unproxiedCluster.getKafkaClientConfiguration(SUPER, "Super"))) {
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

    protected static ConfigurationBuilder proxyConfig(KafkaCluster proxiedCluster,
                                                      Map<String, String> passwords,
                                                      Path rulesFile) {
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
        return proxy(proxiedCluster)
                .addToFilterDefinitions(saslTermination, authorization)
                .addToDefaultFilters(saslTermination.name(), authorization.name())
                .editMatchingVirtualCluster(x -> true)
                .endVirtualCluster();
    }

    protected <Q extends ApiMessage, P extends ApiMessage> Map<String, P> responsesByUser(BaseClusterFixture baseTestCluster,
                                                                                          VersionSpecificVerification<Q, P> scenario) {
        var responsesByUser = new HashMap<String, P>();
        Map<String, Request> requests = scenario.requests(baseTestCluster.topicIds());
        Map<String, KafkaClient> clients = baseTestCluster.authenticatedClients(requests.keySet());
        try {
            if (!clients.keySet().containsAll(requests.keySet())) {
                throw new IllegalStateException("`authenticatedClients` should return a client for every user");
            }
            for (var entry : clients.entrySet()) {
                String user = entry.getKey();
                KafkaClient client = entry.getValue();
                var resp = client.getSync(requests.get(user));

                var r = (P) resp.payload().message();
                responsesByUser.put(user, r);
            }
            return responsesByUser;
        }
        finally {
            clients.values().forEach(KafkaClient::close);
        }
    }

    protected <Q extends ApiMessage, S extends ApiMessage> void verifyApiEqivalence(ReferenceCluster referenceCluster,
                                                                                    ProxiedCluster proxiedCluster,
                                                                                    Equivalence<Q, S> scenario) {
        var unproxiedResponsesByUser = responsesByUser(
                referenceCluster,
                scenario);

        scenario.assertUnproxiedResponses(unproxiedResponsesByUser);
        scenario.assertVisibleSideEffects(referenceCluster);

        var proxiedResponsesByUser = responsesByUser(
                proxiedCluster,
                scenario);

        // assert the responses from the proxied cluster at the same as from the unproxied cluster
        // (modulo clobbbering things like UUIDs which will be unavoidably different)
        Function<S, String> convertAndClobberUserResponse = ((Function<S, ObjectNode>) scenario::convertResponse)
                .andThen(scenario::clobberResponse);
        assertThat(mapValues(proxiedResponsesByUser,
                convertAndClobberUserResponse))
                .as("Expect equivalent response to an unproxied Kafka cluster with the equivalent AuthZ")
                .isEqualTo(mapValues(unproxiedResponsesByUser,
                        convertAndClobberUserResponse));
        // assertions about side effects
        scenario.assertVisibleSideEffects(proxiedCluster);
    }

    protected <Q extends ApiMessage, S extends ApiMessage> void verifyUnsupportedVersion(ProxiedCluster proxiedCluster,
                                                                                         UnsupportedApiVersion<Q, S> unsupported) {

        var proxiedResponsesByUser = responsesByUser(
                proxiedCluster,
                unsupported);

        for (String user : unsupported.passwords().keySet()) {
            assertThat(unsupported.errors(proxiedResponsesByUser.get(user)))
                    .allMatch(e -> e == Errors.UNSUPPORTED_VERSION);
        }
    }

    protected Set<String> topicListing(BaseClusterFixture cluster) {
        try (var admin = Admin.create(cluster.backingCluster().getKafkaClientConfiguration(SUPER, "Super"))) {
            return admin.listTopics().names().toCompletionStage().toCompletableFuture().join();
        }
    }

}