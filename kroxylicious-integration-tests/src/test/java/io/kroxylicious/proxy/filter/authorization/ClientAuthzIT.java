/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.kafka.junit5ext.Name;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test that runs through a sequence of interactions using real high level Clients,
 * recording the outcomes or (expected) exceptions.
 * We expect the client to experience the same outcomes when pointed at Kafka-with-ACLs,
 * or proxy-with-authz (give or take some internal identifiers).
 */
class ClientAuthzIT extends AuthzIT {

    public static final String TOPIC_A = "topicA";
    public static final String TOPIC_B = "topicB";
    public static final String TXN_ID_P = "txnIdP";
    public static final String GROUP_1 = "group1";
    public static final String GROUP_2 = "group2";
    public static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    public static final IntegerSerializer INTEGER_SERIALIZER = new IntegerSerializer();
    public static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    public static final IntegerDeserializer INTEGER_DESERIALIZER = new IntegerDeserializer();

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name in {"%s", "%s"};
                allow User with name = "bob" to CREATE Topic with name = "%s";
                otherwise deny;
                """.formatted(TOPIC_A, TOPIC_B, TOPIC_A));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_B, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TRANSACTIONAL_ID, TXN_ID_P, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_1, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, GROUP_2, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, TOPIC_A, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.CREATE, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(TOPIC_A), aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(TOPIC_A), List.of());
    }

    public sealed interface Outcome<V> permits Success, Fail {
        default boolean isSuccess() {
            return this instanceof Success;
        }

        V value();

        Exception cause();

        default <T> Outcome<T> map(Function<V, T> function) {
            if (isSuccess()) {
                return new Success<>(function.apply(value()));
            }
            else {
                return (Outcome<T>) this;
            }
        }
    }

    public record Success<V>(V value) implements Outcome<V> {
        @Override
        public KafkaException cause() {
            throw new IllegalStateException();
        }
    }

    public record Fail<V>(Exception cause) implements Outcome<V> {

        @NonNull
        public static <T> Fail<T> of(ExecutionException e) {
            if (e.getCause() instanceof KafkaException ke) {
                return new Fail<>(ke);
            }
            throw new RuntimeException(e.getCause());
        }

        public static <T> Outcome<T> of(Exception e) {
            return new Fail<>(e);
        }

        @Override
        public V value() {
            throw new IllegalStateException(cause);
        }
    }

    record AdminContext(String user,
                        Map<String, Object> configs,
                        Admin admin,
                        Consumer<TracedOp> tracer)
            implements AutoCloseable {

        private String clientId() {
            return (String) configs.get(AdminClientConfig.CLIENT_ID_CONFIG);
        }

        private <R> R trace(String op, R result) {
            tracer.accept(new TracedOp(user, clientId(), op, String.valueOf(result)));
            return result;
        }

        @Override
        public void close() {
            Outcome<Void> outcome;
            try {
                admin.close();
                outcome = new Success<>(null);
            }
            catch (Exception e) {
                outcome = Fail.of(e);
            }
            trace("close", outcome);
        }

        Outcome<Uuid> createTopic(String name) {
            Outcome<Uuid> outcome;
            try {
                outcome = new Success<>(admin
                        .createTopics(List.of(new NewTopic(name, 1, (short) 1)))
                        .topicId(name)
                        .get());
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            trace("createTopic", outcome.map(uuid -> null));
            return outcome;
        }

        public Outcome<Void> createPartitions(String topic, int numPartitions) {
            Outcome<Void> outcome;
            try {
                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(numPartitions)))
                        .all()
                        .get();
                outcome = new Success<>(null);
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return trace("createPartitions", outcome);
        }

        public Outcome<TopicDescription> describeTopic(String topic) {
            Outcome<TopicDescription> outcome;
            try {
                outcome = new Success<>(admin
                        .describeTopics(List.of(topic))
                        .topicNameValues()
                        .get(topic)
                        .get());
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            trace("describeTopic", outcome.map(ClientAuthzIT::cleanTopicDescription));
            return outcome;
        }

        public Outcome<Void> deleteTopic(String topicName) {
            return deleteTopic(TopicCollection.ofTopicNames(List.of(topicName)));
        }

        public Outcome<Void> deleteTopic(Uuid topicId) {
            return deleteTopic(TopicCollection.ofTopicIds(List.of(topicId)));
        }

        public Outcome<Void> deleteTopic(TopicCollection topicCollection) {
            Outcome<Void> outcome;
            try {
                outcome = new Success<>(admin
                        .deleteTopics(topicCollection)
                        .all()
                        .get());
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome = Fail.of(e);
            }
            return trace("deleteTopic", outcome);
        }

        public Outcome<Config> describeConfigs(ConfigResource.Type type, String resourceName) {
            Outcome<Config> outcome;
            try {
                var resource = new ConfigResource(type, resourceName);
                outcome = new Success<>(admin
                        .describeConfigs(List.of(resource))
                        .all()
                        .get()
                        .get(resource));
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome = Fail.of(e);
            }
            return trace("describeConfigs", outcome);
        }

        public Outcome<Void> alterConfigs(ConfigResource.Type type, String resourceName, ConfigEntry entry) {
            Outcome<Void> outcome;
            try {
                var resource = new ConfigResource(type, resourceName);
                outcome = new Success<>(admin
                        .incrementalAlterConfigs(Map.of(resource, List.of(
                                new AlterConfigOp(entry, AlterConfigOp.OpType.SET))))
                        .all()
                        .get());
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome = Fail.of(e);
            }
            return trace("alterConfigs", outcome);
        }
    }

    record ProducerContext<V>(String user,
                              Map<String, Object> configs,
                              Producer<String, V> producer,
                              Consumer<TracedOp> tracer)
            implements AutoCloseable {

        private String clientId() {
            return (String) configs.get(AdminClientConfig.CLIENT_ID_CONFIG);
        }

        private <R> R trace(String op, R result) {
            tracer.accept(new TracedOp(user, clientId(), op, String.valueOf(result)));
            return result;
        }

        @Override
        public void close() {
            Outcome<Void> outcome;
            try {
                producer.close();
                outcome = new Success<>(null);
            }
            catch (Exception e) {
                outcome = Fail.of(e);
            }
            trace("close", outcome);
        }

        public Outcome<RecordMetadata> send(String topic, V value) {
            Outcome<RecordMetadata> outcome;
            try {
                outcome = new Success<>(producer.send(new ProducerRecord<>(topic, "context.user()", value))
                        .get());
            }
            catch (RuntimeException e) {
                outcome = Fail.of(e);
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return trace("send", outcome);
        }

        public Outcome<Void> initTransactions() {
            Outcome<Void> outcome;
            try {
                producer.initTransactions();
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("initTransaction", outcome);
        }

        public Outcome<Void> beginTransaction() {
            Outcome<Void> outcome;
            try {
                producer.beginTransaction();
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("beginTransaction", outcome);
        }

        public Outcome<Void> commitTransaction() {
            Outcome<Void> outcome;
            try {
                producer.commitTransaction();
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("commitTransaction", outcome);
        }

        public Outcome<Void> abortTransaction() {
            Outcome<Void> outcome;
            try {
                producer.abortTransaction();
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("abortTransaction", outcome);
        }

        public Outcome<Void> sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata) {
            Outcome<Void> outcome;
            try {
                producer.sendOffsetsToTransaction(offsets, groupMetadata);
                outcome = new Success<>(null);
            }
            catch (RuntimeException e) {
                outcome = Fail.of(e);
            }
            return trace("sendOffsetsToTransaction", outcome);
        }
    }

    record ConsumerContext<K, V>(String user,
                                 Map<String, Object> configs,
                                 org.apache.kafka.clients.consumer.Consumer<K, V> consumer,
                                 Consumer<TracedOp> tracer)
            implements AutoCloseable {

        private String clientId() {
            return (String) configs.get(AdminClientConfig.CLIENT_ID_CONFIG);
        }

        private <R> R trace(String op, R result) {
            tracer.accept(new TracedOp(user, clientId(), op, String.valueOf(result)));
            return result;
        }

        @Override
        public void close() {
            Outcome<Void> outcome;
            try {
                consumer.close();
                outcome = new Success<>(null);
            }
            catch (Exception e) {
                outcome = Fail.of(e);
            }
            trace("close", outcome);
        }

        public Outcome<Void> assign(List<TopicPartition> partitions) {
            Outcome<Void> outcome;
            try {
                // send to topicA
                consumer.assign(partitions);
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("assign", outcome);
        }

        public Outcome<Void> seekToBeginning(List<TopicPartition> partitions) {
            Outcome<Void> outcome;
            try {
                consumer.seekToBeginning(partitions);
                outcome = new Success<>(null);
            }
            catch (RuntimeException e) {
                outcome = Fail.of(e);
            }
            return trace("seekToBeginning", outcome);
        }

        public Outcome<ConsumerRecords<K, V>> poll() {
            Outcome<ConsumerRecords<K, V>> outcome;
            try {
                Duration timeout = Duration.ofSeconds(3);
                ConsumerRecords<K, V> poll = consumer.poll(timeout);

                outcome = new Success(poll);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            trace("poll", outcome.map(PrettyRecords::new));
            return outcome;
        }

        public Outcome<Void> commitSync() {
            Outcome<Void> outcome;
            try {
                // send to topicA
                consumer.commitSync();
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("commitSync", outcome);
        }

        public Outcome<Void> subscribe(String topic) {
            Outcome<Void> outcome;
            try {
                // send to topicA
                consumer.subscribe(List.of(topic));
                outcome = new Success<>(null);
            }
            catch (KafkaException e) {
                outcome = Fail.of(e);
            }
            return trace("subscribe", outcome);
        }

        public Outcome<ConsumerGroupMetadata> groupMetadata() {
            Outcome<ConsumerGroupMetadata> outcome;
            try {
                outcome = new Success<>(consumer.groupMetadata());
            }
            catch (RuntimeException e) {
                outcome = Fail.of(e);
            }
            trace("groupMetadata", outcome.map(m -> new ConsumerGroupMetadata(m.groupId(), m.generationId(), "CLOBBERED", m.groupInstanceId())));
            return outcome;
        }
    }

    static Map<String, Object> adding(Map<String, Object> map, Map<String, Object> and) {
        var result = new HashMap<>(map);
        result.putAll(and);
        return result;
    }

    interface Prog {

        void run(ClientFactory clientFactory) throws InterruptedException;
    }

    record AdminProg() implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {
            String topicA = TOPIC_A;
            String topicB = TOPIC_B;

            String setupUser = ALICE;
            String adminUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var admin = clientFactory.newAdmin(adminUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "admin"))) {
                    admin.createPartitions(topicA, 2);
                    admin.describeTopic(topicA);
                    admin.describeConfigs(ConfigResource.Type.TOPIC, topicA);
                    admin.alterConfigs(ConfigResource.Type.TOPIC, topicA, new ConfigEntry(TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd"));
                    var topicBId = setup.createTopic(topicB).value();

                    admin.deleteTopic(topicBId);
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    record SimpleProg(int numSends) implements Prog {

        @Override
        public void run(ClientFactory clientFactory) {
            String topicA = TOPIC_A;
            TopicPartition topicAPartition0 = new TopicPartition(topicA, 0);
            TopicPartition topicAPartition1 = new TopicPartition(topicA, 1);

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var nonTransactionalProducer = clientFactory.newProducer(producerUser, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "nonTransactionalProducer",
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, AlwaysPartition0Partitioner.class.getName()));
                        var nonGroupedConsumer = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                                ProducerConfig.CLIENT_ID_CONFIG, "nonGroupedConsumer",
                                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {
                    for (int i = 0; i < numSends; i++) {
                        nonTransactionalProducer.send(topicA, i);
                    }

                    nonGroupedConsumer.assign(List.of(topicAPartition0, topicAPartition1));
                    // poll(),
                    nonGroupedConsumer.seekToBeginning(List.of(topicAPartition0, topicAPartition1));
                    nonGroupedConsumer.poll();
                    nonGroupedConsumer.commitSync();
                }

                setup.deleteTopic(topicA);
            }
        }
    }

    static class TransactionalProg implements Prog {

        @Override
        @SuppressWarnings("java:S2925") // Thread.sleep
        public void run(ClientFactory clientFactory) throws InterruptedException {
            String topicA = TOPIC_A;
            TopicPartition topicAPartition0 = new TopicPartition(topicA, 0);
            TopicPartition topicAPartition1 = new TopicPartition(topicA, 1);

            String setupUser = ALICE;
            String producerUser = ALICE;
            String consumerUser = ALICE;

            try (var setup = clientFactory.newAdmin(setupUser, Map.of(AdminClientConfig.CLIENT_ID_CONFIG, "setup"))) {
                setup.createTopic(topicA);
                try (var transactionalProducer = clientFactory.newProducer(producerUser, INTEGER_SERIALIZER, Map.of(
                        ProducerConfig.CLIENT_ID_CONFIG, "transactionalProducer",
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG, TXN_ID_P,
                        ProducerConfig.PARTITIONER_CLASS_CONFIG, AlwaysPartition0Partitioner.class.getName()))) {

                    transactionalProducer.initTransactions();

                    // Commit
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 2);
                    transactionalProducer.commitTransaction();

                    // Abort
                    transactionalProducer.beginTransaction();
                    transactionalProducer.send(topicA, 3);
                    transactionalProducer.abortTransaction();

                    try (var consumer = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG, "consumer",
                            ConsumerConfig.GROUP_ID_CONFIG, GROUP_1,
                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_1 + "-instance-1",
                            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {
                        consumer.subscribe(TOPIC_A);
                        consumer.poll();
                        consumer.seekToBeginning(List.of(topicAPartition0, topicAPartition1));
                        consumer.poll();
                        consumer.commitSync();
                    }

                    // Commit
                    transactionalProducer.beginTransaction().value();
                    var sent = transactionalProducer.send(topicA, 4).value().offset();
                    transactionalProducer.commitTransaction().value();

                    try (var consumer2 = clientFactory.newConsumer(consumerUser, INTEGER_DESERIALIZER, Map.of(
                            ConsumerConfig.CLIENT_ID_CONFIG, "consumer2",
                            ConsumerConfig.GROUP_ID_CONFIG, GROUP_2,
                            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, GROUP_2 + "-instance-1",
                            ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1))) {

                        consumer2.subscribe(TOPIC_A);
                        ConsumerRecords<String, Integer> records;
                        Instant start = Instant.now();
                        while (true) {
                            if (Instant.now().minusSeconds(10).isAfter(start)) {
                                throw new RuntimeException("timed out waiting for records");
                            }
                            records = consumer2.poll().value();
                            List<ConsumerRecord<String, Integer>> records1 = records.records(topicAPartition0);
                            if (!records1.isEmpty()
                                    && records1.get(records1.size() - 1).offset() >= sent) {
                                break;
                            }
                            Thread.sleep(100);
                        }

                        var offsets = records.nextOffsets();
                        assertThat(offsets).isNotEmpty();
                        assertThat(offsets.get(topicAPartition0).offset()).isGreaterThan(0);
                        var metadata = consumer2.groupMetadata().value();
                        assertThat(metadata.generationId()).isGreaterThan(0);
                        transactionalProducer.beginTransaction();
                        assertThat(transactionalProducer.sendOffsetsToTransaction(offsets, metadata).value()).isNull();
                        transactionalProducer.commitTransaction();

                    }
                }
                setup.deleteTopic(topicA);
            }
        }
    }

    public record Actor(String username, String password,
                        Map<String, Object> adminConfigOverrides,
                        Map<String, Object> producerConfigOverrides,
                        Map<String, Object> consumerConfigOverrides) {

        Actor withAdminConfigOverrides(Map<String, Object> overrides) {
            return new Actor(username, password, overrides, producerConfigOverrides, consumerConfigOverrides);
        }

        Actor withProducerConfigOverrides(Map<String, Object> overrides) {
            return new Actor(username, password, adminConfigOverrides, overrides, consumerConfigOverrides);
        }

        Actor withConsumerConfigOverrides(Map<String, Object> overrides) {
            return new Actor(username, password, adminConfigOverrides, producerConfigOverrides, overrides);
        }

        Actor withAdminConfigOverrides(String key, Object value) {
            return withAdminConfigOverrides(Map.of(key, value));
        }

        Actor withProducerConfigOverrides(String key, Object value) {
            return withProducerConfigOverrides(Map.of(key, value));
        }

        Actor withConsumerConfigOverrides(String key, Object value) {
            return withConsumerConfigOverrides(Map.of(key, value));
        }
    }

    record ClientFactory(Map<String, Actor> actors,
                         BaseClusterFixture cluster,
                         List<TracedOp> tracedOps) {

        ClientFactory(List<Actor> actors, BaseClusterFixture cluster) {
            this(actors.stream().collect(Collectors.toMap(Actor::username, Function.identity())),
                    cluster,
                    new ArrayList<>());
        }

        AdminContext newAdmin(String user,
                              Map<String, Object> configs) {
            Actor actor = actors.get(user);
            Map<String, Object> clientProperties = adding(cluster.getKafkaClientConfiguration(user, actor.password()),
                    configs);
            clientProperties = adding(clientProperties,
                    actor.adminConfigOverrides());
            Admin admin = Admin.create(clientProperties);
            return new AdminContext(user, configs, admin, tracedOps::add);
        }

        <V> ProducerContext<V> newProducer(String user,
                                           Serializer<V> valueSerializer,
                                           Map<String, Object> configs) {
            Actor actor = actors.get(user);
            Map<String, Object> clientProperties = adding(cluster.getKafkaClientConfiguration(user, actor.password()),
                    configs);
            clientProperties = adding(clientProperties,
                    actor.producerConfigOverrides());
            KafkaProducer<String, V> producer = new KafkaProducer<>(clientProperties,
                    new StringSerializer(), valueSerializer);
            return new ProducerContext<>(user, configs, producer, tracedOps::add);
        }

        <V> ConsumerContext<String, V> newConsumer(String user,
                                                   Deserializer<V> valueDeserializer,
                                                   Map<String, Object> configs) {
            Actor actor = actors.get(user);
            Map<String, Object> clientProperties = adding(cluster.getKafkaClientConfiguration(user, actor.password()),
                    configs);
            clientProperties = adding(clientProperties,
                    actor.consumerConfigOverrides());
            KafkaConsumer<String, V> consumer = new KafkaConsumer<>(clientProperties,
                    new StringDeserializer(), valueDeserializer);
            return new ConsumerContext<>(user, configs, consumer, tracedOps::add);
        }
    }

    List<Arguments> shouldEnforceAccessToTopics() {
        var alice = new Actor(ALICE, "Alice", Map.of(), Map.of(), Map.of());
        var eve = new Actor(EVE, "Eve", Map.of(), Map.of(), Map.of());
        // TODO support new consumer protocol
        // Arguments newConsumerProtocol = Arguments.of(new TransactionalProg(), List.of(alice.withConsumerConfigOverrides(Map.of(
        // ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT))),
        // eve.withConsumerConfigOverrides(Map.of(
        // ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT)))));
        return List.of(
                Arguments.of(new AdminProg(), List.of(alice, eve)),
                Arguments.of(new SimpleProg(1), List.of(alice, eve)),
                Arguments.of(new SimpleProg(1), List.of(
                        alice.withProducerConfigOverrides(ProducerConfig.ACKS_CONFIG, "0"),
                        eve.withProducerConfigOverrides(ProducerConfig.ACKS_CONFIG, "0"))),
                Arguments.of(new SimpleProg(100), List.of(
                        alice.withProducerConfigOverrides(Map.of(
                                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 100,
                                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false)),
                        eve.withProducerConfigOverrides(Map.of(
                                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 100,
                                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false)))),
                Arguments.of(new TransactionalProg(), List.of(
                        alice.withConsumerConfigOverrides(Map.of(
                                ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000)),
                        eve.withConsumerConfigOverrides(Map.of(
                                ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
                                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10_000)))));
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(Prog prog, List<Actor> actors) throws Exception {

        List<TracedOp> referenceResults;
        try (BaseClusterFixture cluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster)) {
            referenceResults = traceExecution(actors, cluster, prog);
        }

        List<TracedOp> proxiedResults;
        try (var cluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            proxiedResults = traceExecution(actors, cluster, prog);
        }

        assertThat(proxiedResults).isEqualTo(referenceResults);
    }

    record TracedOp(String user, String varName, String op, String result) {}

    record PrettyRecords<K, V>(ConsumerRecords<K, V> records) {
        @Override
        public String toString() {

            List<TopicPartition> partitions = records.partitions().stream()
                    .sorted(Comparator.comparing(TopicPartition::topic)
                            .thenComparing(TopicPartition::partition))
                    .toList();
            var sb = new StringBuilder("ConsumerRecords{count=").append(records.count()).append(',');
            boolean firstPartition = true;
            for (var tp : partitions) {
                if (!firstPartition) {
                    sb.append(',');
                }
                firstPartition = false;
                sb.append(tp).append("=[");
                boolean firstRecord = true;
                for (var r : records.records(tp)) {
                    if (!firstRecord) {
                        sb.append(',');
                    }
                    firstRecord = false;
                    sb.append("{offset=").append(r.offset());
                    sb.append(",key=").append(r.key());
                    sb.append(",value=").append(r.value());
                    sb.append('}');
                }
                sb.append(']');
            }
            return sb.append('}').toString();
        }
    }

    /**
     * Cleans/normalises the hostnames and port numbers from a topic description,
     * so they're comparable between clusters for assertion purposes.
     * @param topicDescription The topic description
     * @return A new cleaned up topic description
     */
    static TopicDescription cleanTopicDescription(TopicDescription topicDescription) {
        return new TopicDescription(
                topicDescription.name(),
                topicDescription.isInternal(),
                topicDescription.partitions().stream().map(ClientAuthzIT::cleanTopicPartitionInfo).toList(),
                topicDescription.authorizedOperations(),
                topicDescription.topicId());
    }

    static TopicPartitionInfo cleanTopicPartitionInfo(TopicPartitionInfo topicPartitionInfo) {
        if (topicPartitionInfo.elr() == null) {
            return new TopicPartitionInfo(
                    topicPartitionInfo.partition(),
                    cleanNode(topicPartitionInfo.leader()),
                    cleanNodes(topicPartitionInfo.replicas()),
                    cleanNodes(topicPartitionInfo.isr()));
        }
        else {
            return new TopicPartitionInfo(
                    topicPartitionInfo.partition(),
                    cleanNode(topicPartitionInfo.leader()),
                    cleanNodes(topicPartitionInfo.replicas()),
                    cleanNodes(topicPartitionInfo.isr()),
                    cleanNodes(topicPartitionInfo.elr()),
                    cleanNodes(topicPartitionInfo.lastKnownElr()));
        }
    }

    static List<Node> cleanNodes(List<Node> nodes) {
        return nodes.stream()
                .map(ClientAuthzIT::cleanNode)
                .toList();
    }

    static Node cleanNode(Node node) {
        return new Node(node.id(),
                "host", // node.host(),
                0, // node.port(),
                node.rack(),
                node.isFenced());
    }

    private List<TracedOp> traceExecution(List<Actor> actors,
                                          BaseClusterFixture cluster,
                                          Prog prog)
            throws InterruptedException {

        ClientFactory clientFactory = new ClientFactory(actors, cluster);
        prog.run(clientFactory);
        return clientFactory.tracedOps();
    }

    public static class AlwaysPartition0Partitioner implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return 0;
        }

        @Override
        public void close() {
            // stateless
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // stateless
        }
    }

}
