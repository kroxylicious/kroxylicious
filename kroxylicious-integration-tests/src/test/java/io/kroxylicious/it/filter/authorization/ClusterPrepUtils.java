/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.awaitility.core.ConditionFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

final class ClusterPrepUtils {
    static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60)).pollDelay(Duration.ofMillis(100));

    private ClusterPrepUtils() {
        // static utility class
    }

    /**
     * Uses the supplied admin to create topic(s) and apply the given ACL bindings, awaiting
     * the topics to become visible with partition leaders before return to the caller.
     *
     * @param admin admin client
     * @param topicNames set of topics
     * @param bindings set of bindings
     * @return map of newly created topics
     */
    static Map<String, Uuid> createTopicsAndAcls(Admin admin,
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
                            .filter(ClusterPrepUtils::topicPartitionsHaveALeader)
                            .map(TopicDescription::name)
                            .collect(Collectors.toSet());
                    assertThat(topicNames).containsExactlyInAnyOrderElementsOf(readyTopics);
                });
        return topicNames.stream().collect(Collectors.toMap(Function.identity(), topicName -> res.topicId(topicName).toCompletionStage().toCompletableFuture().join()));
    }

    /**
     * Uses the supplied admin to delete topic(s) and remove the given ACL bindings, awaiting
     * the topics to disappear before returning to the caller.
     *
     * @param admin admin client
     * @param topicNames set of topics
     * @param bindings set of bindings
     */
    static void deleteTopicsAndAcls(Admin admin,
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
     * Tests whether all partitions for the given topics have a leader assigned
     *
     * @param admin admin client
     * @param topicNames topic names
     * @return true if all topic partitions have a leader.
     */
    static boolean allTopicPartitionsHaveALeader(Admin admin, List<String> topicNames) {
        Map<String, TopicDescription> join = admin.describeTopics(topicNames).allTopicNames().toCompletionStage().toCompletableFuture().join();
        return join.values().stream()
                .allMatch(ClusterPrepUtils::topicPartitionsHaveALeader);
    }

    private static Stream<TopicDescription> describeTopics(List<String> topics, Admin admin) {
        return admin.describeTopics(topics).topicNameValues().values().stream()
                .map(f -> f.toCompletionStage().toCompletableFuture())
                .filter(ClusterPrepUtils::filterUnknownTopics)
                .map(CompletableFuture::join);
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
