/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.kroxylicious.testing.kafka.clients.CloseableAdmin;
import io.kroxylicious.testing.kafka.clients.CloseableConsumer;
import io.kroxylicious.testing.kafka.clients.CloseableProducer;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * base class for high-level authorization integration tests where we wish to:
 * 1. use the official high level kafka clients
 * 2. execute realistic client workflows against a proxied cluster with authz and reference cluster with ACLs
 * 3. trace the result of every high-level client operation, capturing result values or exceptions
 * 4. compare the traces, expecting the behaviour to be identical (except for data like timestamps, unique ids etc)
 */
public abstract class AbstractTracingIT extends AuthzIT {

    /**
     * A prog represents a sequence of client interactions with a cluster
     * All clients used by the test should be obtained via the ClientFactory so that
     * their results can be traced.
     */
    public interface Prog {

        void run(ClientFactory clientFactory) throws InterruptedException;
    }

    /**
     * A test Actor. Contains the credentials required to authenticate as this actor along with
     * configuration overrides for the high-level Kafka Clients.
     * @param username username
     * @param password password
     * @param adminConfigOverrides config overrides to be applied when creating {@link Admin} instances
     * @param producerConfigOverrides config overrides to be applied when creating {@link Producer} instances
     * @param consumerConfigOverrides config overrides to be applied when creating {@link KafkaConsumer} instances
     */
    public record Actor(String username, String password,
                        Map<String, Object> adminConfigOverrides,
                        Map<String, Object> producerConfigOverrides,
                        Map<String, Object> consumerConfigOverrides) {

        public Actor withProducerConfigOverrides(Map<String, Object> overrides) {
            return new Actor(username, password, adminConfigOverrides, overrides, consumerConfigOverrides);
        }

        public Actor withConsumerConfigOverrides(Map<String, Object> overrides) {
            return new Actor(username, password, adminConfigOverrides, producerConfigOverrides, overrides);
        }

        public Actor withProducerConfigOverrides(String key, Object value) {
            return withProducerConfigOverrides(Map.of(key, value));
        }

    }

    /**
     * Runs a Prog against a reference cluster and a proxied cluster. Compares the resulting traces expecting them
     * to match.
     *
     * @param prog the prog to execute
     * @param actors the actors available to the prog
     * @param authorizationRulesFile path to an authorization rules file, this will be installed into the proxied cluster
     */
    protected void assertProgTraceMatches(Prog prog, List<Actor> actors, Path authorizationRulesFile) throws Exception {

        List<TracedOp> referenceResults;
        try (BaseClusterFixture cluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster)) {
            referenceResults = traceExecution(actors, cluster, prog);
        }

        List<TracedOp> proxiedResults;
        try (var cluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, authorizationRulesFile)) {
            proxiedResults = traceExecution(actors, cluster, prog);
        }

        assertThat(proxiedResults).isEqualTo(referenceResults);
    }

    /**
     * The outcome of an operation
     * @param <V> the value type in case of a successful outcome
     */
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

    public record Success<V>(V value) implements Outcome<V> {
        @Override
        public KafkaException cause() {
            throw new IllegalStateException();
        }
    }

    public record TracedOp(String user, String varName, String op, String result) {}

    private List<TracedOp> traceExecution(List<Actor> actors,
                                          BaseClusterFixture cluster,
                                          Prog prog)
            throws InterruptedException {

        ClientFactory clientFactory = new ClientFactory(actors, cluster);
        prog.run(clientFactory);
        return clientFactory.tracedOps();
    }

    public record AdminContext(String user,
                               Map<String, Object> configs,
                               Admin admin,
                               Consumer<TracedOp> tracer)
            implements AutoCloseable {

        public static final int UUID_STRING_LENGTH = 36;

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

        public Outcome<Uuid> createTopic(String name) {
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
            trace("describeTopic", outcome.map(AdminContext::cleanTopicDescription));
            return outcome;
        }

        public Outcome<Void> deleteTopic(String topicName) {
            return deleteTopic(TopicCollection.ofTopicNames(List.of(topicName)));
        }

        public Outcome<Void> deleteTopic(Uuid topicId) {
            return deleteTopic(TopicCollection.ofTopicIds(List.of(topicId)));
        }

        public Outcome<Void> deleteGroups(List<String> groups) {
            Outcome<Void> outcome;
            try {
                outcome = new Success<>(admin
                        .deleteConsumerGroups(groups)
                        .all()
                        .get(10, TimeUnit.SECONDS));
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome = Fail.of(e);
            }
            catch (TimeoutException e) {
                outcome = Fail.of(e);
            }
            return trace("deleteGroups", outcome);
        }

        public Outcome<Void> deleteTopic(TopicCollection topicCollection) {
            Outcome<Void> outcome;
            try {
                outcome = new Success<>(admin
                        .deleteTopics(topicCollection)
                        .all()
                        .get(10, TimeUnit.SECONDS));
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                outcome = Fail.of(e);
            }
            catch (TimeoutException e) {
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

        public Outcome<ConsumerGroupDescription> describeConsumerGroup(String groupId) {
            Outcome<ConsumerGroupDescription> outcome;
            try {
                outcome = new Success<>(admin
                        .describeConsumerGroups(List.of(groupId))
                        .all()
                        .get().get(groupId));
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            trace("describeConsumerGroup", outcome.map(AdminContext::cleanConsumerGroupDescription));
            return outcome;
        }

        public Outcome<TransactionDescription> describeTransaction(String transactionalId) {
            Outcome<TransactionDescription> outcome;
            try {
                outcome = new Success<>(admin
                        .describeTransactions(List.of(transactionalId))
                        .all()
                        .get().get(transactionalId));
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            trace("describeTransaction", outcome.map(TransactionDescription::state));
            return outcome;
        }

        public Outcome<Collection<TransactionListing>> listTransactions() {
            Outcome<Collection<TransactionListing>> outcome;
            try {
                outcome = new Success<>(admin
                        .listTransactions()
                        .all()
                        .get());
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            trace("listTransactions", outcome.map(transactionListings -> transactionListings.stream().map(TransactionListing::transactionalId).toList()));
            return outcome;
        }

        public Outcome<Collection<GroupListing>> listGroups() {
            Outcome<Collection<GroupListing>> outcome;
            try {
                outcome = new Success<>(admin
                        .listGroups()
                        .all()
                        .get());
            }
            catch (ExecutionException e) {
                outcome = Fail.of(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            trace("listGroups", outcome.map(transactionListings -> transactionListings.stream().map(GroupListing::groupId).toList()));
            return outcome;
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
                    topicDescription.partitions().stream().map(AdminContext::cleanTopicPartitionInfo).toList(),
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

        static ConsumerGroupDescription cleanConsumerGroupDescription(ConsumerGroupDescription groupDescription) {
            return new ConsumerGroupDescription(
                    groupDescription.groupId(),
                    groupDescription.isSimpleConsumerGroup(),
                    groupDescription.members().stream().map(AdminContext::cleanGroupMember).toList(),
                    groupDescription.partitionAssignor(),
                    groupDescription.type(),
                    groupDescription.groupState(),
                    cleanNode(groupDescription.coordinator()),
                    groupDescription.authorizedOperations(),
                    groupDescription.groupEpoch(),
                    groupDescription.targetAssignmentEpoch());
        }

        private static MemberDescription cleanGroupMember(MemberDescription memberDescription) {
            return new MemberDescription(
                    memberDescription.consumerId().substring(0,
                            memberDescription.consumerId().length() - UUID_STRING_LENGTH),
                    memberDescription.groupInstanceId(),
                    memberDescription.clientId(),
                    memberDescription.host(),
                    memberDescription.assignment(),
                    memberDescription.targetAssignment(),
                    memberDescription.memberEpoch(),
                    memberDescription.upgraded());
        }

        static List<Node> cleanNodes(List<Node> nodes) {
            return nodes.stream()
                    .map(AdminContext::cleanNode)
                    .toList();
        }

        static Node cleanNode(Node node) {
            return new Node(node.id(),
                    "host", // node.host(),
                    0, // node.port(),
                    node.rack(),
                    node.isFenced());
        }
    }

    public record ClientFactory(Map<String, Actor> actors,
                                BaseClusterFixture cluster,
                                List<TracedOp> tracedOps) {

        public ClientFactory(List<Actor> actors, BaseClusterFixture cluster) {
            this(actors.stream().collect(Collectors.toMap(Actor::username, Function.identity())),
                    cluster,
                    new ArrayList<>());
        }

        public AdminContext newAdmin(String user,
                                     Map<String, Object> configs) {
            Actor actor = actors.get(user);
            Map<String, Object> clientProperties = adding(cluster.getKafkaClientConfiguration(user, actor.password()),
                    configs);
            clientProperties = adding(clientProperties,
                    actor.adminConfigOverrides());
            Admin admin = new CloseableAdmin(Admin.create(clientProperties));
            return new AdminContext(user, configs, admin, tracedOps::add);
        }

        public <V> ProducerContext<V> newProducer(String user,
                                                  Serializer<V> valueSerializer,
                                                  Map<String, Object> configs) {
            Actor actor = actors.get(user);
            Map<String, Object> clientProperties = adding(cluster.getKafkaClientConfiguration(user, actor.password()),
                    configs);
            clientProperties = adding(clientProperties,
                    actor.producerConfigOverrides());
            KafkaProducer<String, V> producer = new KafkaProducer<>(clientProperties,
                    new StringSerializer(), valueSerializer);
            return new ProducerContext<>(user, configs, new CloseableProducer<>(producer), tracedOps::add);
        }

        public <V> ConsumerContext<String, V> newConsumer(String user,
                                                          Deserializer<V> valueDeserializer,
                                                          Map<String, Object> configs) {
            Actor actor = actors.get(user);
            Map<String, Object> clientProperties = adding(cluster.getKafkaClientConfiguration(user, actor.password()),
                    configs);
            clientProperties = adding(clientProperties,
                    actor.consumerConfigOverrides());
            KafkaConsumer<String, V> consumer = new KafkaConsumer<>(clientProperties,
                    new StringDeserializer(), valueDeserializer);
            return new ConsumerContext<>(user, configs, new CloseableConsumer<>(consumer), tracedOps::add);
        }

        static Map<String, Object> adding(Map<String, Object> map, Map<String, Object> and) {
            var result = new HashMap<>(map);
            result.putAll(and);
            return result;
        }

    }

    public record ConsumerContext<K, V>(String user,
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

                outcome = new Success<>(poll);
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

    public record PrettyRecords<K, V>(ConsumerRecords<K, V> records) {
        @NonNull
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

    public record ProducerContext<V>(String user,
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
            catch (KafkaException | IllegalStateException e) {
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
            catch (KafkaException | IllegalStateException e) {
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
            catch (KafkaException | IllegalStateException e) {
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
}
