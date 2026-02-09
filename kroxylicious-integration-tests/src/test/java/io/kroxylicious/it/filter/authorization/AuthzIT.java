/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.kroxylicious.authorizer.provider.acl.AclAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.it.BaseIT;
import io.kroxylicious.it.testplugins.SaslPlainTermination;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.RequestFactory;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerConfig;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

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
public abstract class AuthzIT extends BaseIT {

    static final Logger LOG = LoggerFactory.getLogger(AuthzIT.class);

    protected static final ObjectMapper MAPPER = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    public static final String SUPER = "super";
    public static final String ALICE = "alice";
    public static final String BOB = "bob";
    public static final String EVE = "eve";
    public static final String SUPER_PASSWORD = "Super";

    public static final Map<String, String> PASSWORDS = Map.of(
            ALICE, "Alice",
            BOB, "Bob",
            EVE, "Eve");
    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60)).pollDelay(Duration.ofMillis(100));

    @SaslMechanism(principals = {
            @SaslMechanism.Principal(user = SUPER, password = SUPER_PASSWORD),
            @SaslMechanism.Principal(user = ALICE, password = "Alice"),
            @SaslMechanism.Principal(user = BOB, password = "Bob"),
            @SaslMechanism.Principal(user = EVE, password = "Eve")
    })
    @BrokerConfig(name = "authorizer.class.name", value = "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
    // ANONYMOUS is the broker
    @BrokerConfig(name = "super.users", value = "User:ANONYMOUS;User:super")
    @Name("kafkaClusterWithAuthz")
    static KafkaCluster kafkaClusterWithAuthz;

    @Name("kafkaClusterNoAuthz")
    static KafkaCluster kafkaClusterNoAuthz;

    Map<String, Uuid> topicIdsInUnproxiedCluster;
    Map<String, Uuid> topicIdsInProxiedCluster;

    static AclBinding allowAllOnGroup(String user, String groupId) {
        return new AclBinding(
                new ResourcePattern(ResourceType.GROUP, groupId, PatternType.LITERAL),
                new AccessControlEntry("User:" + user, "*",
                        AclOperation.ALL, AclPermissionType.ALLOW));
    }

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

        Q requestData(String user, BaseClusterFixture baseTestCluster);

        default ObjectNode convertResponse(S response) {
            return (ObjectNode) KafkaApiMessageConverter.responseConverterFor(apiKey().messageType).writer().apply(response, apiVersion());
        }

        default Stream<Errors> errors(S response) {
            return convertResponse(response).findValues("errorCode").stream().map(node -> {
                if (node.isShort()) {
                    return Errors.forCode(node.shortValue());
                }
                throw new IllegalStateException("Node called errorCode did not have a short value");
            });
        }

        default Map<String, Request> requests(BaseClusterFixture baseTestCluster) {
            return Map.of(
                    ALICE, newRequest(requestData(ALICE, baseTestCluster)),
                    BOB, newRequest(requestData(BOB, baseTestCluster)),
                    EVE, newRequest(requestData(EVE, baseTestCluster)));
        }

        default Request newRequest(ApiMessage aliceRequest) {
            return getRequest(apiVersion(), aliceRequest);
        }

        /**
         * @param response The response to a test request.
         * @return Determine whether the test request that resulted in the given response should be retried.
         * (This can be necessary if the broker is not initially in the needed state, for example
         * being the leader of a partition in the request).
         */
        default boolean needsRetry(S response) {
            return false;
        }
    }

    /**
     * An abstraction for creating requests which depend on factors not known until just before the request is to be made.
     * <br/>
     * While we want to specify the general "shape" of a request to be defined in the test method parameter supplier
     * (i.e. the thing named in the {@code @MethodSource}), an actual request with that shape can
     * depend on things, like topic ids, which are specific to the cluster (which might be a ReferenceCluster or a ProxiedCluster).
     * This interface allow decoupling this cluster-dependence from the point where we define a shape of a test request.
     * @param <Q> The type of the request.
     */
    interface RequestTemplate<Q> {
        Q request(String user, BaseClusterFixture clusterFixture);
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

        public abstract String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonResponse);

        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
            // do nothing by default
        }

        public void assertUnproxiedResponses(Map<String, S> unproxiedResponsesByUser) {
            // do nothing by default
        }

        @Override
        public void verifyBehaviour(ReferenceCluster referenceCluster, ProxiedCluster proxiedCluster) {
            verifyApiEquivalence(
                    referenceCluster,
                    proxiedCluster,
                    this);
        }

        public void prepareCluster(BaseClusterFixture cluster) {
            // do nothing by default
        }

        @SuppressWarnings("unused") // cluster is used in subclasses
        public Object observedVisibleSideEffects(BaseClusterFixture cluster) {
            return null;
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
        public Q requestData(String user, BaseClusterFixture clusterFixture) {
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
        public Map<String, Request> requests(BaseClusterFixture clusterFixture) {
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

    protected static ArrayNode sortArray(ObjectNode root, String arrayProperty, String sortProperty, String... thenSortProperties) {
        JsonNode topics = root.path(arrayProperty);
        if (topics.isArray()) {
            Comparator<JsonNode> comparing = Comparator.comparing(itemNode -> itemNode.get(sortProperty).textValue(),
                    Comparator.nullsFirst(String::compareTo));
            for (var thenSortProperty : thenSortProperties) {
                Comparator<JsonNode> thenComparator = Comparator.comparing(itemNode -> itemNode.get(thenSortProperty).textValue(),
                        Comparator.nullsFirst(String::compareTo));
                comparing = comparing.thenComparing(thenComparator);
            }
            var sortedTopics = topics.valueStream().sorted(
                    comparing)
                    .toList();
            root.putArray(arrayProperty).addAll(sortedTopics);
            return (ArrayNode) root.get(arrayProperty);
        }
        return null;
    }

    protected static String prettyJsonString(final JsonNode root) {
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

    protected static JsonNode maybeClobberedString(JsonNode uuid) {
        if (uuid != null && uuid.isTextual()) {
            return TextNode.valueOf("CLOBBERED");
        }
        return uuid;
    }

    protected static void clobberUuid(ObjectNode root, String propertyName) {
        root.replace(propertyName, maybeClobberedUuid(root.get(propertyName)));
    }

    protected static void clobberString(ObjectNode root, String propertyName) {
        root.replace(propertyName, maybeClobberedString(root.get(propertyName)));
    }

    protected static void clobberInt(ObjectNode root, String propertyName, int replacement) {
        root.replace(propertyName, maybeClobberedInt(root.get(propertyName), replacement));
    }

    private static JsonNode maybeClobberedInt(JsonNode jsonNode, int replacement) {
        if (jsonNode != null && jsonNode.isNumber()) {
            return new IntNode(replacement);
        }
        else {
            return jsonNode;
        }
    }

    protected static void ensureCoordinators(Producer<String, String> producer,
                                             String transactionalId,
                                             Admin admin) {
        AWAIT.alias("await until transaction coordinators are ready")
                .untilAsserted(() -> {
                    producer.initTransactions();
                    producer.beginTransaction();
                    producer.send(new ProducerRecord<>("top", "", "")).get();
                    var coordId = admin.describeTransactions(List.of(transactionalId)).all().toCompletionStage().toCompletableFuture()
                            .join().get(transactionalId).coordinatorId();
                    producer.abortTransaction();
                    assertThat(coordId).isNotEqualTo(-1);
                });
    }

    protected static Map<String, Uuid> prepCluster(Admin admin,
                                                   List<String> topicNames,
                                                   List<AclBinding> bindings) {
        if (!bindings.isEmpty()) {
            admin.createAcls(bindings).all()
                    .toCompletionStage().toCompletableFuture().join();
        }
        var res = admin.createTopics(topicNames.stream().map(topicName -> new NewTopic(topicName, 1, (short) 1)).toList());
        res.all().toCompletionStage().toCompletableFuture().join();
        AWAIT.alias("await until topics visible and partitions have leader")
                .untilAsserted(() -> {
                    var readyTopics = describeTopics(topicNames, admin)
                            .filter(AuthzIT::topicPartitionsHaveALeader)
                            .map(TopicDescription::name)
                            .collect(Collectors.toSet());
                    assertThat(topicNames).containsExactlyInAnyOrderElementsOf(readyTopics);
                });
        return topicNames.stream().collect(Collectors.toMap(Function.identity(), topicName -> res.topicId(topicName).toCompletionStage().toCompletableFuture().join()));
    }

    private static Stream<TopicDescription> describeTopics(List<String> topics, Admin admin) {
        return admin.describeTopics(topics).topicNameValues().values().stream()
                .map(f -> f.toCompletionStage().toCompletableFuture())
                .filter(AuthzIT::filterUnknownTopics)
                .map(CompletableFuture::join);
    }

    public static boolean allTopicPartitionsHaveALeader(Admin admin, List<String> topicNames) {
        Map<String, TopicDescription> join = admin.describeTopics(topicNames).allTopicNames().toCompletionStage().toCompletableFuture().join();
        return join.values().stream()
                .allMatch(AuthzIT::topicPartitionsHaveALeader);
    }

    protected static void deleteTopicsAndAcls(Admin admin,
                                              List<String> topicNames,
                                              List<AclBinding> bindings) {

        try {
            KafkaFuture<Void> result = admin.deleteTopics(TopicCollection.ofTopicNames(topicNames))
                    .all();
            result.toCompletionStage().toCompletableFuture().join();
        }
        catch (CompletionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
        }
        finally {
            if (!bindings.isEmpty()) {
                var filters = bindings.stream().map(AclBinding::toFilter).toList();
                admin.deleteAcls(filters).all()
                        .toCompletionStage().toCompletableFuture().join();
            }

            AWAIT.alias("await visibility of topic removal.")
                    .untilAsserted(() -> assertThat(describeTopics(topicNames, admin)).isEmpty());

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
        var handshakeResponse = (SaslHandshakeResponseData) client.getSync(getRequest(ApiKeys.SASL_HANDSHAKE.latestVersion(),
                new SaslHandshakeRequestData().setMechanism("PLAIN")))
                .payload().message();
        assertThat(Errors.forCode(handshakeResponse.errorCode())).isEqualTo(Errors.NONE);

        byte[] bytes = (username + "\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8);
        var authenticateResponse = (SaslAuthenticateResponseData) client.getSync(getRequest(
                ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                new SaslAuthenticateRequestData()
                        .setAuthBytes(bytes)))
                .payload().message();
        assertThat(Errors.forCode(authenticateResponse.errorCode())).isEqualTo(Errors.NONE);
    }

    static Request getRequest(short apiVersion, ApiMessage request) {
        return new Request(
                ApiKeys.forId(request.apiKey()),
                apiVersion,
                "test",
                request);
    }

    static Request getRequest(short apiVersion, Function<Short, ApiMessage> requestFn) {
        ApiMessage request = requestFn.apply(apiVersion);
        return getRequest(apiVersion, request);
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

    protected <Q extends ApiMessage, S extends ApiMessage> Map<String, S> responsesByUser(BaseClusterFixture baseTestCluster,
                                                                                          VersionSpecificVerification<Q, S> scenario) {
        var responsesByUser = new HashMap<String, S>();
        Map<String, Request> requests = scenario.requests(baseTestCluster);
        Map<String, KafkaClient> clients = baseTestCluster.authenticatedClients(requests.keySet());
        try {
            if (!clients.keySet().containsAll(requests.keySet())) {
                throw new IllegalStateException("`authenticatedClients` should return a client for every user");
            }
            for (var entry : clients.entrySet()) {
                String user = entry.getKey();
                KafkaClient client = entry.getValue();
                Request request = requests.get(user);
                S r;
                do {
                    r = sendRequestAndGetResponse(baseTestCluster, user, request, client);
                } while (scenario.needsRetry(r));
                responsesByUser.put(user, r);
            }
            return responsesByUser;
        }
        finally {
            clients.values().forEach(KafkaClient::close);
        }
    }

    private <S extends ApiMessage> S sendRequestAndGetResponse(BaseClusterFixture baseTestCluster, String user, Request request, KafkaClient client) {
        LOG.atDebug().setMessage("{} {}{} >> {}")
                .addArgument(user)
                .addArgument(request.apiKeys())
                .addArgument(() -> prettyJsonString(
                        KafkaApiMessageConverter.requestConverterFor(request.apiKeys().messageType).writer().apply(request.message(), request.apiVersion())))
                .addArgument(baseTestCluster.name())
                .log();
        Response resp = client.getSync(request);

        S responseMessage = (S) resp.payload().message();
        LOG.atDebug().setMessage("{} {}{} << {}")
                .addArgument(user)
                .addArgument(request.apiKeys())
                .addArgument(() -> prettyJsonString(
                        KafkaApiMessageConverter.responseConverterFor(request.apiKeys().messageType).writer().apply(responseMessage, request.apiVersion())))
                .addArgument(baseTestCluster.name())
                .log();
        return responseMessage;
    }

    protected <Q extends ApiMessage, S extends ApiMessage> void verifyApiEquivalence(ReferenceCluster referenceCluster,
                                                                                     ProxiedCluster proxiedCluster,
                                                                                     Equivalence<Q, S> scenario) {

        scenario.prepareCluster(referenceCluster);

        var unproxiedResponsesByUser = responsesByUser(
                referenceCluster,
                scenario);

        scenario.assertUnproxiedResponses(unproxiedResponsesByUser);

        scenario.prepareCluster(proxiedCluster);

        var proxiedResponsesByUser = responsesByUser(
                proxiedCluster,
                scenario);

        // assert the responses from the proxied cluster at the same as from the unproxied cluster
        // (modulo clobbbering things like UUIDs which will be unavoidably different)
        BiFunction<S, BaseClusterFixture, String> convertAndClobberUserResponse = (response, cl) -> {
            var node = scenario.convertResponse(response);
            return scenario.clobberResponse(cl, node);
        };
        assertThatJson(mapValues(proxiedResponsesByUser,
                x -> convertAndClobberUserResponse.apply(x, proxiedCluster)))
                .as("Expect proxied response to be the same as the reference response with equivalent AuthZ")
                .isEqualTo(mapValues(unproxiedResponsesByUser,
                        x1 -> convertAndClobberUserResponse.apply(x1, referenceCluster)));

        AWAIT.alias("assertions about visible side effects").untilAsserted(() -> {
            var referenceObservation = scenario.observedVisibleSideEffects(referenceCluster);
            scenario.assertVisibleSideEffects(referenceCluster);

            var proxiedObservation = scenario.observedVisibleSideEffects(proxiedCluster);
            scenario.assertVisibleSideEffects(proxiedCluster);

            assertThat(proxiedObservation)
                    .as("Expect side effects visible on the proxied cluster to be the same as the reference cluster")
                    .isEqualTo(referenceObservation);
        });
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
        try (var admin = Admin.create(cluster.backingCluster().getKafkaClientConfiguration(SUPER, SUPER_PASSWORD))) {
            return admin.listTopics().names().toCompletionStage().toCompletableFuture().join();
        }
    }

    protected Map<TopicPartition, Long> offsets(BaseClusterFixture cluster, String groupId) {
        try (var admin = Admin.create(cluster.backingCluster().getKafkaClientConfiguration(SUPER, SUPER_PASSWORD))) {
            Map<TopicPartition, OffsetAndMetadata> join = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().toCompletionStage()
                    .toCompletableFuture().join();
            return join.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
        }
    }

    private static boolean filterUnknownTopics(CompletableFuture<TopicDescription> f) {
        try {
            f.join();
            return true;
        }
        catch (CompletionException ce) {
            if (ce.getCause() instanceof UnknownTopicOrPartitionException) {
                return false;
            }
            throw new RuntimeException(ce);
        }
    }

    private static boolean topicPartitionsHaveALeader(TopicDescription td) {
        return td.partitions().stream().allMatch(p -> p.leader() != null);
    }
}
